#!/usr/bin/env bash
set -euo pipefail

OUT_HDFS="${1:-hdfs:///prepared/sector_map.csv}"
TMP="/tmp/sp500_constituents_financials.csv"
URL="https://raw.githubusercontent.com/datasets/s-and-p-companies-financials/c9f83a9c/data/constituents-financials.csv"

echo "[1/5] Downloading sector source..."
curl -L --fail --silent --show-error "$URL" -o "$TMP"

echo "[2/5] Extracting ticker + sector..."
python3 - "$TMP" "/tmp/sector_map.csv" <<'PY'
import csv, sys
src, out = sys.argv[1:3]

# The source is a public S&P 500 constituent/financial snapshot.
# We only retain the metadata needed by the project.
rows = []
with open(src, newline="", encoding="utf-8-sig") as f:
    r = csv.DictReader(f)
    required = {"Symbol", "Sector"}
    if not required.issubset(r.fieldnames or []):
        raise SystemExit(f"Missing required columns. Found: {r.fieldnames}")
    for row in r:
        ticker = (row["Symbol"] or "").strip()
        sector = (row["Sector"] or "").strip()
        if ticker and sector:
            rows.append((ticker, sector))

# Normalise the two share-class ticker spellings that can occur in S&P data.
# The project should only retain mappings for tickers that actually exist
# in its 505-series price corpus.
normalise = {
    "BRK.B": "BRK.B",
    "BF.B": "BF.B",
}

dedup = {}
for ticker, sector in rows:
    ticker = normalise.get(ticker, ticker)
    if ticker in dedup and dedup[ticker] != sector:
        raise SystemExit(f"Conflicting sectors for {ticker}: {dedup[ticker]} vs {sector}")
    dedup[ticker] = sector

with open(out, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["ticker", "sector"])
    for ticker in sorted(dedup):
        w.writerow([ticker, dedup[ticker]])

print(f"Created {len(dedup)} unique ticker-sector mappings")
PY

echo "[3/5] Showing first rows..."
head -10 /tmp/sector_map.csv

echo "[4/5] Uploading to HDFS..."
hdfs dfs -rm -f "$OUT_HDFS" >/dev/null 2>&1 || true
hdfs dfs -put -f /tmp/sector_map.csv "$OUT_HDFS"

echo "[5/5] Verifying HDFS file..."
echo "Path: $OUT_HDFS"
hdfs dfs -cat "$OUT_HDFS" | head -10
echo "Row count (excluding header):"
hdfs dfs -cat "$OUT_HDFS" | tail -n +2 | wc -l
