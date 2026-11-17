"""Validate the curated project and prepare its delivery folder (standard library).

--check validates without building; --prepare permits a review package without
report.pdf. A final submission.zip is produced only when the report is present.
"""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import re
import shutil
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[1]
VERSION = "v2026-15th-nov-2016"
INCLUDE = ("README.md", "report.pdf", "notebooks", "models", "prepared", "scripts",
           "src", "sql", "config", "docs", "evidence", "report")
JUNK = {"__pycache__", ".ipynb_checkpoints", ".git"}
TEXT = {".md", ".py", ".sh", ".json", ".cells", ".hql", ".csv", ".tsv", ".txt", ".log", ".ipynb"}
SECRET = re.compile(r"(?:AKIA|ASIA)[0-9A-Z]{16}|BEGIN (?:RSA |OPENSSH )?PRIVATE KEY|aws_secret_access_key\s*[=:]|aws_session_token\s*[=:]", re.I)
PERSONAL = re.compile(r"[A-Za-z]:\\(?:Users|Programming)|/Users/[a-z]", re.I)


def string(value):
    return "".join(value) if isinstance(value, list) else value or ""


def source_cells():
    result = []
    for p in sorted((ROOT / "notebooks/cells").glob("*.cells")):
        kind, buf = None, []
        for line in p.read_text(encoding="utf8").splitlines() + ["#@@ end"]:
            if line.startswith("#@@ "):
                if kind:
                    result.append(("markdown" if kind == "md" else "code", "\n".join(buf).strip("\n")))
                kind, buf = line[4:].strip(), []
            else:
                buf.append(line)
    return result


def selected_files():
    files = []
    for name in INCLUDE:
        p = ROOT / name
        candidates = [p] if p.is_file() else p.rglob("*") if p.is_dir() else []
        for f in candidates:
            if not f.is_file() or any(x in JUNK for x in f.relative_to(ROOT).parts):
                continue
            if f.suffix.lower() in {".crc", ".pyc", ".zip", ".pem", ".bak"}:
                continue
            if f.name.endswith("_source.ipynb"):
                continue
            files.append(f)
    return sorted(files)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--prepare", action="store_true", help="allow missing report only; label resulting package REVIEW")
    args = parser.parse_args()
    fails, warns = [], []

    def check(ok, message):
        print(("OK   " if ok else "FAIL ") + message)
        if not ok:
            fails.append(message)

    def warn(message):
        warns.append(message)
        print("WARN " + message)

    report = ROOT / "report.pdf"
    has_report = report.is_file() and report.stat().st_size > 100
    if has_report:
        check(report.read_bytes().startswith(b"%PDF-"), "report.pdf has a PDF header (layout/word count require manual review)")
    elif args.prepare:
        warn("report.pdf is missing: folder and ZIP will be REVIEW ONLY, not a complete submission")
    else:
        check(False, "report.pdf is missing")

    files = selected_files()
    nb = None
    try:
        nb = json.loads((ROOT / "notebooks/inf2006_pipeline.ipynb").read_text(encoding="utf8"))
        errors = [i for i, c in enumerate(nb["cells"]) for o in c.get("outputs", [])
                  if o.get("output_type") == "error" or "An error was encountered" in string(o.get("text"))]
        check(not errors, f"executed notebook has no saved errors: {sorted(set(errors))}")
        check(all(c.get("outputs") for c in nb["cells"] if c["cell_type"] == "code"), "all code cells retain execution outputs")
        check([(c["cell_type"], string(c["source"]).strip("\n")) for c in nb["cells"]] == source_cells(), ".cells sources exactly match the executed notebook")
        outputs = "\n".join(string(o.get("text")) for c in nb["cells"] for o in c.get("outputs", []))
        check("34 of 34 checks passed" in outputs, "historical 34/34 verification output retained")
        check("fresh session    : True" in outputs and "RELOAD VERDICT: ALL PASSED" in outputs, "historical fresh-session reload output retained")
        summary = json.loads((ROOT / "evidence/final_run/run_summary.json").read_text(encoding="utf8"))
        digest = hashlib.sha256((ROOT / "notebooks/inf2006_pipeline.ipynb").read_bytes()).hexdigest()
        check(digest == summary["notebook_sha256"], "notebook is byte-identical to the selected EMR export")
        check(summary["training_application_id"] != summary["reload_application_id"], "training and reload application IDs differ")
        warn("execution counts reset for the recorded fresh-session reload; original notebook is preserved")
    except (OSError, ValueError, KeyError) as ex:
        check(False, f"notebook/evidence unreadable: {ex}")

    for task in ("direction", "high_vol"):
        try:
            base = ROOT / "models" / VERSION / task
            meta = json.loads(next((base / "metadata").glob("part-*")).read_text(encoding="utf8"))
            stages = sorted((base / "stages").iterdir())
            uids = []
            for stage in stages:
                sm = json.loads(next((stage / "metadata").glob("part-*")).read_text(encoding="utf8"))
                uids.append(sm["uid"])
                if "VectorAssembler" not in sm["class"]:
                    parts = list((stage / "data").glob("*.parquet"))
                    check(bool(parts), f"{task}/{stage.name}: learned parameters present")
            check(len(stages) == 4 and uids == meta["paramMap"]["stageUids"], f"{task}: four stages match saved PipelineModel metadata")
            for sub in (f"{task}/test", f"pred_{task}"):
                check(bool(list((ROOT / "prepared" / VERSION / "reference" / sub).glob("*.parquet"))), f"reload dataset present: {sub}")
        except (OSError, ValueError, KeyError, StopIteration) as ex:
            check(False, f"model {task} incomplete: {ex}")

    secret_hits, personal_hits, raw_hits, corrupt = [], [], [], []
    for p in files:
        rel = p.relative_to(ROOT).as_posix()
        if re.search(r"(?:all_stocks_5yrs?\.csv|\.export\.CSV)$", p.name, re.I):
            raw_hits.append(rel)
        if p.suffix in TEXT and p.name != "build_submission_zip.py":
            text = p.read_text(encoding="utf8", errors="replace")
            if SECRET.search(text):
                secret_hits.append(rel)
            if PERSONAL.search(text):
                personal_hits.append(rel)
        if p.suffix == ".parquet":
            with p.open("rb") as f:
                first = f.read(4)
                f.seek(-4, 2)
                last = f.read(4)
            if first != b"PAR1" or last != b"PAR1":
                corrupt.append(rel)
    check(not secret_hits, f"credential scan: {secret_hits}")
    check(not personal_hits, f"personal-machine paths: {personal_hits}")
    check(not raw_hits, f"raw dataset scan: {raw_hits}")
    check(not corrupt, f"Parquet header/footer integrity: {corrupt}")
    try:
        with (ROOT / "evidence/final_run/stage1_hdfs_inventory.csv").open(encoding="utf8") as f:
            cloud = list(csv.DictReader(f))
        with (ROOT / "evidence/local/q1_2016/inventory.csv").open(encoding="utf8") as f:
            local = {r["file"]: r for r in csv.DictReader(f)}
        identity = len(cloud) == len(local) == 92 and len({Path(r["hdfs_path"]).name for r in cloud}) == 92
        for row in cloud:
            original = local.get(Path(row["hdfs_path"]).name)
            identity = identity and original is not None
            if original:
                identity = identity and row["sha256"] == original["sha256"] and row["bytes"] == original["bytes"]
                identity = identity and int(row["lines"]) == int(original["rows"]) + (1 if original["source"] == "prices" else 0)
        check(identity, "all 92 HDFS file checksums, bytes and rows reconcile with the local input inventory")
        def audit(p):
            rows = {}
            with p.open(encoding="utf8") as f:
                for line in f:
                    key, n, tone, lag = line.rstrip("\n").split("\t")
                    if key.isdigit():
                        rows[key] = (int(n), float(tone), int(lag))
            return rows
        remote_audit = audit(ROOT / "evidence/final_run/stage2_streaming_output.tsv")
        local_audit = audit(ROOT / "evidence/local/q1_2016/streaming_gdelt_audit.tsv")
        agreement = remote_audit.keys() == local_audit.keys() and len(remote_audit) == 91
        agreement = agreement and all(a[0] == local_audit[k][0] and a[2] == local_audit[k][2] and abs(a[1] - local_audit[k][1]) < 1e-3 for k, a in remote_audit.items())
        check(agreement, "91-day Streaming output agrees with local event/lag counts and tone tolerance")
    except (OSError, ValueError, KeyError) as ex:
        check(False, f"inventory/streaming reconciliation failed: {ex}")
    for need in ("evidence/final_run/run_log.csv", "evidence/final_run/stage1_hdfs_inventory.csv",
                 "evidence/final_run/notebook_text_outputs.txt", "evidence/gdelt_manifest/q1_2016/manifest.tsv",
                 "docs/RUN_ON_EMR.md", "docs/EVIDENCE_INDEX.md", "scripts/reload_saved_models.py"):
        check((ROOT / need).is_file(), f"required asset: {need}")
    broken = []
    for doc in [ROOT / "README.md"] + list((ROOT / "docs").glob("*.md")):
        for target in re.findall(r"\[[^\]]+\]\(([^)]+)\)", doc.read_text(encoding="utf8")):
            target = target.split("#")[0]
            if not target or "://" in target or target.startswith("mailto:"):
                continue
            if not (doc.parent / target).exists():
                broken.append(f"{doc.relative_to(ROOT)} -> {target}")
    check(not broken, f"documentation links resolve: {broken}")
    warn("retained run has incomplete full-pipeline timing/configuration comparisons; see docs/EVIDENCE_INDEX.md")
    print(f"\n{len(files)} files, {sum(p.stat().st_size for p in files)/1e6:.2f} MB; {len(fails)} FAIL, {len(warns)} WARN")
    if fails or args.check:
        return 1 if fails else 0

    delivery = ROOT / "submission"
    assert delivery.resolve().parent == ROOT and delivery.name == "submission"
    if delivery.exists():
        if delivery.is_symlink():
            raise RuntimeError("Refusing a linked delivery directory")
        shutil.rmtree(delivery)
    delivery.mkdir()
    for p in files:
        dest = delivery / p.relative_to(ROOT)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(p, dest)
    status = "REPORT PRESENT: review report content and known evidence limitations before upload." if has_report else "REVIEW ONLY: report.pdf is missing. Do not submit this ZIP yet."
    (delivery / "SUBMISSION_STATUS.md").write_text("# Submission status\n\n" + status + "\n\nSee README.md and docs/EVIDENCE_INDEX.md. The recovery archive is deliberately excluded.\n", encoding="utf8")
    entries = [{"path": p.relative_to(delivery).as_posix(), "bytes": p.stat().st_size,
                "sha256": hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(delivery.rglob("*")) if p.is_file()]
    (delivery / "SUBMISSION_MANIFEST.json").write_text(json.dumps({"version": VERSION, "report_present": has_report, "files": entries}, indent=2) + "\n", encoding="utf8")
    archive = ROOT / ("submission.zip" if has_report else "submission-review.zip")
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as z:
        for p in sorted(delivery.rglob("*")):
            if p.is_file():
                z.write(p, p.relative_to(delivery).as_posix())
    with zipfile.ZipFile(archive) as z:
        if z.testzip() is not None:
            raise RuntimeError("Delivery ZIP failed CRC check")
        for e in entries:
            if hashlib.sha256(z.read(e["path"])).hexdigest() != e["sha256"]:
                raise RuntimeError("Delivery hash mismatch: " + e["path"])
    print(f"Prepared submission/ and {archive.name}: {archive.stat().st_size/1e6:.2f} MB; CRC and all manifest hashes verified")
    return 0


if __name__ == "__main__":
    sys.exit(main())
