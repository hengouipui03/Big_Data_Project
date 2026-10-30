"""Task 1: raw-corpus inventory, checksums and quality profile (pure Python, streaming).

Usage: python -I src/pipeline/t1_profile_raw.py [raw_dir] [out_dir] [sources_json] [gdelt_dir] [gdelt_manifest]
Writes inventory.csv (per-file provenance: source URL, acquisition time, bytes, rows,
sha256) and quality_summary.json.

Provenance comes from config/sources.json (download URL template, version, optional exact
acquisition time). When acquired_utc is not given there, the file's modification time on
disk is used as the acquisition time.
"""
import csv, collections, datetime, hashlib, json, os, sys

RAW = sys.argv[1] if len(sys.argv) > 1 else "data/raw"
OUT = sys.argv[2] if len(sys.argv) > 2 else "evidence/local"
SOURCES = sys.argv[3] if len(sys.argv) > 3 else "config/sources.json"
GDELT_DIR = sys.argv[4] if len(sys.argv) > 4 else os.path.join(RAW, "gdelt")
GDELT_MANIFEST = sys.argv[5] if len(sys.argv) > 5 else None
os.makedirs(OUT, exist_ok=True)

with open(SOURCES) as f:
    sources = {k: v for k, v in json.load(f).items() if not k.startswith("_")}

# exact download times written by src/acquire/download_raw.py, if that script was used
ACQ_LOG = os.path.join(OUT, "acquisition_log.csv")
acq = {}
if os.path.exists(ACQ_LOG):
    with open(ACQ_LOG, newline="") as f:
        for r in csv.DictReader(f):
            acq[(r["source"], r["file"])] = r

# The range downloader records the official ZIP checksum and exact acquisition
# time in a TSV manifest.  It is intentionally separate from acquisition_log.csv
# because a full GDELT profile can contain many daily files.
gdelt_acq = {}
if GDELT_MANIFEST and os.path.exists(GDELT_MANIFEST):
    with open(GDELT_MANIFEST, newline="") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            if r["status"] == "OK":
                gdelt_acq[r["date"] + ".export.CSV"] = r


def provenance(src, name, path):
    """Source URL, version and acquisition time for one raw file.

    Priority: acquisition_log.csv (exact) > acquired_utc in sources.json > file mtime on disk.
    """
    meta = sources.get(src, {})
    url = meta.get("url_template", "").format(file=name)
    if (src, name) in acq:
        return acq[(src, name)]["url"], meta.get("version", ""), acq[(src, name)]["acquired_utc"]
    if src == "gdelt" and name in gdelt_acq:
        return gdelt_acq[name]["url"], meta.get("version", ""), gdelt_acq[name]["acquired_utc"]
    acquired = meta.get("acquired_utc") or datetime.datetime.fromtimestamp(
        os.path.getmtime(path), datetime.timezone.utc).isoformat(timespec="seconds")
    return url, meta.get("version", ""), acquired


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def profile_prices(path):
    n = dups = 0
    seen = set()
    empty = collections.Counter()
    ohlc_bad = nonpos = 0
    per = collections.Counter()
    dmin, dmax, upto_2017 = "9999", "0000", 0
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            n += 1
            key = (r["Name"], r["date"])
            dups += key in seen
            seen.add(key)
            per[r["Name"]] += 1
            dmin, dmax = min(dmin, r["date"]), max(dmax, r["date"])
            upto_2017 += r["date"] <= "2017-12-29"
            try:
                o, h, l, c, v = (float(r[k]) for k in ("open", "high", "low", "close", "volume"))
            except ValueError:
                for k in ("open", "high", "low", "close", "volume"):
                    if r[k] == "":
                        empty[k] += 1
                continue
            ohlc_bad += h < max(o, c, l) or l > min(o, c, h)
            nonpos += min(o, h, l, c) <= 0 or v < 0
    lens = sorted(per.values())
    return {
        "rows": n, "tickers": len(per), "date_min": dmin, "date_max": dmax,
        "rows_up_to_2017-12-29": upto_2017, "duplicate_ticker_date": dups,
        "rows_with_empty_field": dict(empty), "ohlc_inconsistent_rows": ohlc_bad,
        "nonpositive_rows": nonpos,
        "series_length_min_median_max": [lens[0], lens[len(lens) // 2], lens[-1]],
        "short_series_lt_500": sorted(k for k, v in per.items() if v < 500),
    }


def profile_gdelt(path):
    rows = bad_cols = 0
    added = collections.Counter()
    lag_gt0 = 0
    empty_url = 0
    for line in open(path, encoding="utf-8", errors="replace"):
        rows += 1
        c = line.rstrip("\n").split("\t")
        if len(c) != 58:
            bad_cols += 1
            continue
        added[c[56]] += 1
        lag_gt0 += c[1] != c[56]
        empty_url += c[57] == ""
    return {"rows": rows, "rows_not_58_cols": bad_cols, "rows_event_date_ne_dateadded": lag_gt0,
            "empty_source_url": empty_url, "dateadded_values": dict(added)}


inventory, summary = [], {"prices": None, "gdelt": {}}
now = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
for src in ("prices", "gdelt"):
    d = GDELT_DIR if src == "gdelt" else os.path.join(RAW, src)
    if not os.path.isdir(d):
        continue
    names = sorted(os.listdir(d))
    # The brief-linked price snapshot has a canonical filename.  Prefer it if
    # it is present so a leftover experimental CSV cannot alter the corpus.
    if src == "prices" and "all_stocks_5yr.csv" in names:
        names = ["all_stocks_5yr.csv"]
    for name in names:
        p = os.path.join(d, name)
        if not os.path.isfile(p):
            continue
        prof = profile_prices(p) if src == "prices" else profile_gdelt(p)
        if src == "prices":
            summary["prices"] = prof
        else:
            summary["gdelt"][name] = prof
        url, version, acquired = provenance(src, name, p)
        inventory.append({"source": src, "file": name, "source_url": url, "version": version,
                          "acquired_utc": acquired, "bytes": os.path.getsize(p),
                          "rows": prof["rows"], "sha256": sha256(p), "profiled_utc": now})

with open(os.path.join(OUT, "inventory.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(inventory[0]))
    w.writeheader()
    w.writerows(inventory)
with open(os.path.join(OUT, "quality_summary.json"), "w") as f:
    json.dump(summary, f, indent=2)
print(json.dumps(summary["prices"], indent=2))
print(f"gdelt files: {len(summary['gdelt'])}, rows: {sum(v['rows'] for v in summary['gdelt'].values()):,}")
