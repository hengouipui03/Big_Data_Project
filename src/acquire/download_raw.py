#!/usr/bin/env python3
"""Task 1: download the raw sources named in the project brief and record provenance.

Usage:
    python -I src/acquire/download_raw.py prices
    python -I src/acquire/download_raw.py gdelt 20151201 20151207      # inclusive date range
    python -I src/acquire/download_raw.py gdelt 20151201               # a single day

What it does
  * prices : downloads all_stocks_5yr.csv into data/raw/prices/
  * gdelt  : downloads <yyyymmdd>.export.CSV.zip for each day in the range, verifies it is a
             real zip, unzips it into data/raw/gdelt/, and deletes the zip
  * appends one line per file to evidence/local/acquisition_log.csv with the exact download
    URL, UTC time, size and sha256 of the bytes received. t1_profile_raw.py reads this log,
    so the inventory shows the true acquisition time, not the file timestamp.

Files that already exist are skipped (delete them to re-download). No credentials needed.
"""
import csv, datetime, hashlib, io, json, os, sys, urllib.request, zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SOURCES = os.path.join(ROOT, "config", "sources.json")
RAW = os.path.join(ROOT, "data", "raw")
LOG = os.path.join(ROOT, "evidence", "local", "acquisition_log.csv")

with open(SOURCES) as f:
    sources = {k: v for k, v in json.load(f).items() if not k.startswith("_")}


def fetch(url):
    """Download url and return (bytes, utc_time_string)."""
    print(f"  downloading {url}")
    t = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
    req = urllib.request.Request(url, headers={"User-Agent": "INF2006-big-data-project"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read(), t


def log(source, file, url, acquired, nbytes, digest):
    os.makedirs(os.path.dirname(LOG), exist_ok=True)
    new = not os.path.exists(LOG)
    with open(LOG, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["source", "file", "url", "acquired_utc", "downloaded_bytes", "downloaded_sha256"])
        w.writerow([source, file, url, acquired, nbytes, digest])


def get_prices():
    d = os.path.join(RAW, "prices")
    os.makedirs(d, exist_ok=True)
    url = sources["prices"]["url_template"]
    name = url.rsplit("/", 1)[-1]
    path = os.path.join(d, name)
    if os.path.exists(path):
        print(f"  skip {name} (already downloaded)")
        return
    data, t = fetch(url)
    with open(path, "wb") as f:
        f.write(data)
    log("prices", name, url, t, len(data), hashlib.sha256(data).hexdigest())
    print(f"  saved {name}: {len(data):,} bytes")


def get_gdelt(start, end):
    d = os.path.join(RAW, "gdelt")
    os.makedirs(d, exist_ok=True)
    day = datetime.datetime.strptime(start, "%Y%m%d")
    last = datetime.datetime.strptime(end, "%Y%m%d")
    while day <= last:
        name = day.strftime("%Y%m%d") + ".export.CSV"
        path = os.path.join(d, name)
        if os.path.exists(path):
            print(f"  skip {name} (already downloaded)")
        else:
            url = sources["gdelt"]["url_template"].format(file=name)
            data, t = fetch(url)
            if not zipfile.is_zipfile(io.BytesIO(data)):
                print(f"  !! {name}: server did not return a zip file (day may be missing). Skipped.")
            else:
                with zipfile.ZipFile(io.BytesIO(data)) as z:
                    z.extract(name, d)
                log("gdelt", name, url, t, len(data), hashlib.sha256(data).hexdigest())
                print(f"  saved {name}: zip {len(data):,} bytes -> {os.path.getsize(path):,} bytes")
        day += datetime.timedelta(days=1)


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in ("prices", "gdelt"):
        print(__doc__)
        sys.exit(1)
    if sys.argv[1] == "prices":
        get_prices()
    else:
        start = sys.argv[2]
        end = sys.argv[3] if len(sys.argv) > 3 else start
        get_gdelt(start, end)
    print("done. provenance appended to", os.path.relpath(LOG, ROOT))
