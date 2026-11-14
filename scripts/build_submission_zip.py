"""Pre-submission checker and ZIP builder (standard library only).

    python -I scripts/build_submission_zip.py --check            # run every check, build nothing
    python -I scripts/build_submission_zip.py                    # check, then write submission.zip (only if no FAIL)

Why not `git archive`: the Spark ML model folders contain `data/` sub-folders that git ignored for a while, and the
models/evidence pulled from S3 may not be committed yet. The ZIP is therefore built from the working folder with an
explicit allow-list, and the checks below mirror the brief's "Submission requirements" list.
"""
import glob
import json
import os
import re
import sys
import zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
OUT = "submission.zip"
MODEL_VERSION = "v2026-10-29-q1-2016"

INCLUDE = ["report.pdf", "README.md", "SEC_Filing_Inventory.csv", "notebooks", "models", "scripts", "src", "sql", "config", "docs",
           "evidence", "report"]
EXCLUDE_DIRS = {"__pycache__", ".ipynb_checkpoints", ".git"}
EXCLUDE_SUFFIX = (".crc", ".pyc", ".zip", ".pem")
EXCLUDE_PREFIX = ("data/", "work/", "superseded/", "tmp/", "state/", "evidence/local/model_improvement_v1/", "report/report_source/")
TEXT_SUFFIX = (".md", ".py", ".sh", ".json", ".cells", ".hql", ".csv", ".tsv", ".txt", ".log", ".ipynb")
SECRET = re.compile(r"AKIA[0-9A-Z]{16}|aws_secret_access_key|BEGIN (RSA |OPENSSH )?PRIVATE KEY|aws_session_token|ASIA[0-9A-Z]{16}", re.I)
MACHINE = re.compile(r"[A-Za-z]:\\Users\\|/Users/[a-z]|/home/[a-z]+/|[A-Z]:\\Programming|C:\\Users")

results = []


def rec(level, msg):
    results.append((level, msg))
    print(f"{level:4s} {msg}")


def files():
    out = []
    for item in INCLUDE:
        if os.path.isfile(item):
            out.append(item)
        elif os.path.isdir(item):
            for d, dirs, fs in os.walk(item):
                dirs[:] = [x for x in dirs if x not in EXCLUDE_DIRS]
                for f in fs:
                    p = os.path.join(d, f).replace("\\", "/")
                    if p.startswith(EXCLUDE_PREFIX) or p.endswith(EXCLUDE_SUFFIX):
                        continue
                    out.append(p)
    return sorted(out)


FILES = files()
print(f"{len(FILES)} files would be shipped\n")

# 1 report
if os.path.exists("report.pdf"):
    rec("OK", "report.pdf exists (check by hand: cover with team code, names, IDs, date and main-text word count; headings 1-7; Appendices A-D; <= 3,000 words)")
else:
    rec("WARN", "report.pdf is not here yet (written by teammates; add it to the project root before the final build)")

# 2 notebook
nbp = "notebooks/inf2006_pipeline.ipynb"
if os.path.exists(nbp):
    nb = json.load(open(nbp, encoding="utf-8"))
    code = [c for c in nb["cells"] if c["cell_type"] == "code"]
    errs = [(i, o["ename"]) for i, c in enumerate(nb["cells"]) for o in c.get("outputs", []) if o.get("output_type") == "error"]
    # sparkmagic prints failures as stream text ("An error was encountered") instead of error outputs
    sm = [i for i, c in enumerate(nb["cells"]) for o in c.get("outputs", []) if "An error was encountered" in "".join(o.get("text", ""))]
    empty = [i for i, c in enumerate(nb["cells"]) if c["cell_type"] == "code" and not "".join(c["source"]).strip() and c.get("outputs")]
    counts = [c.get("execution_count") for c in code if c.get("execution_count") is not None]
    mono = all(b > a for a, b in zip(counts, counts[1:]))
    apps = set(re.findall(r"application_\d+_\d+", json.dumps(nb)))
    rec("FAIL" if (errs or sm) else "OK", f"notebook: cells ending in an error: {sorted(set([i for i, _ in errs] + sm))}")
    rec("FAIL" if empty else "OK", f"notebook: code cells with output but no source: {empty}")
    rec("WARN" if not mono else "OK", f"notebook: execution counts strictly increasing: {mono}" + ("" if mono else " (outputs come from more than one run/session - re-run Restart & Run All and export again)"))
    rec("WARN" if len(apps) > 2 else "OK", f"notebook: distinct Spark application ids in outputs: {sorted(apps)} (expect the training session and the reload session)")
else:
    rec("FAIL", f"{nbp} is missing")

# 3 notebook source == .cells source (so a rebuild cannot silently drop anything)
try:
    cells = []
    for path in sorted(glob.glob("notebooks/cells/*.cells")):
        kind, buf = None, []
        for line in open(path, encoding="utf-8").read().split("\n") + ["#@@ end"]:
            if line.startswith("#@@ "):
                if kind:
                    cells.append(("\n".join(buf)).strip("\n"))
                kind, buf = line[4:].strip(), []
            else:
                buf.append(line)
    nbs = [("".join(c["source"])).strip("\n") for c in nb["cells"]]
    missing = [i for i, c in enumerate(nbs) if c not in cells]
    rec("WARN" if missing else "OK", f"notebook cells whose source is not in notebooks/cells/*.cells (ad-hoc edits): {missing}")
except Exception as ex:  # noqa
    rec("WARN", f"could not compare notebook with .cells: {ex}")

# 4 models complete
STAGE_NEEDS_DATA = ("Imputer", "StandardScaler", "LogisticRegression", "RandomForest", "GBT")
mroot = f"models/{MODEL_VERSION}"
for task in ("direction", "high_vol"):
    base = f"{mroot}/{task}"
    if not os.path.isdir(base):
        rec("FAIL", f"model folder missing: {base}")
        continue
    bad = []
    stages = sorted(glob.glob(f"{base}/stages/*"))
    for s in stages:
        name = os.path.basename(s)
        if not os.path.isdir(f"{s}/metadata"):
            bad.append(f"{name}: no metadata/")
        if any(k in name for k in STAGE_NEEDS_DATA) and not glob.glob(f"{s}/data/*.parquet"):
            bad.append(f"{name}: no data/*.parquet (learned parameters missing)")
    if not os.path.isdir(f"{base}/metadata"):
        bad.append("no metadata/")
    rec("FAIL" if bad or not stages else "OK", f"model {task}: {len(stages)} stages" + (f"; PROBLEMS: {bad}" if bad else "; learned-parameter files present"))

# 5 text scans
secret_hits, machine_hits, big = [], [], []
for p in FILES:
    if os.path.getsize(p) > 50 * 1024 * 1024:
        big.append(p)
    if p.endswith(TEXT_SUFFIX) and os.path.getsize(p) < 20 * 1024 * 1024 and p != "scripts/build_submission_zip.py":
        t = open(p, encoding="utf-8", errors="ignore").read()
        if SECRET.search(t):
            secret_hits.append(p)
        if MACHINE.search(t):
            machine_hits.append(p)
rec("FAIL" if secret_hits else "OK", f"credential patterns in: {secret_hits}")
rec("FAIL" if machine_hits else "OK", f"machine-specific paths in: {machine_hits}")
rec("FAIL" if big else "OK", f"files > 50 MB (raw data?): {big}")
raw = [p for p in FILES if re.search(r"(all_stocks_5yrs?\.csv|\.export\.CSV)$", p, re.I)]
rec("FAIL" if raw else "OK", f"raw dataset files in the ZIP: {raw}")
acct = [p for p in FILES if p.endswith(TEXT_SUFFIX) and re.search(r"\b\d{12}\b", open(p, encoding="utf-8", errors="ignore").read(20000000)) and "310947418945" in open(p, encoding="utf-8", errors="ignore").read(20000000)]
rec("WARN" if acct else "OK", f"AWS account id / personal bucket name appears in {len(acct)} files (not a credential, but consider whether you want it in the ZIP)")

# 6 relative references in docs resolve
pat = re.compile(r"`((?:evidence|docs|scripts|src|sql|config|notebooks|models|report)/[A-Za-z0-9_./\-]+)`")
broken = {}
for p in ["README.md"] + sorted(glob.glob("docs/*.md")) + ["config/emr/README.md", "config/emr/EMR_cluster_history.md"]:
    if os.path.exists(p):
        for m in pat.finditer(open(p, encoding="utf-8").read()):
            ref = m.group(1).rstrip(".,;:")
            if "<" in ref or "*" in ref or ref.endswith("/"):
                continue
            if not os.path.exists(ref):
                broken.setdefault(ref, set()).add(p)
rec("FAIL" if broken else "OK", f"documentation references to files that do not exist: {len(broken)}")
for ref, ps in sorted(broken.items()):
    print(f"       {ref}   <- {sorted(ps)}")

# 7 evidence sanity
for need in ("evidence/emr_run_2026-11-04_q1-2016/run_log.csv", "evidence/gdelt_manifest/q1_2016/manifest.tsv", "evidence/local/q1_2016/inventory.csv"):
    rec("OK" if os.path.exists(need) else "FAIL", f"evidence file present: {need}")

fails = [m for l, m in results if l == "FAIL"]
warns = [m for l, m in results if l == "WARN"]
print(f"\n{len(fails)} FAIL, {len(warns)} WARN")
if "--check" in sys.argv:
    sys.exit(1 if fails else 0)
if fails:
    print("Not building the ZIP: fix the FAIL lines first (or run with --check to see them only).")
    sys.exit(1)
with zipfile.ZipFile(OUT, "w", zipfile.ZIP_DEFLATED) as z:
    for p in FILES:
        z.write(p, p)
print(f"wrote {OUT}: {len(FILES)} files, {os.path.getsize(OUT) / 1e6:.1f} MB (paths are relative to the ZIP root)")
