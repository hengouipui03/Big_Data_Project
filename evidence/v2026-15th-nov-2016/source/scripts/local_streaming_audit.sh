#!/usr/bin/env bash
# Reproduce the GDELT Streaming audit locally for a directory of daily files.
#
# Usage: bash scripts/local_streaming_audit.sh INPUT_DIR OUTPUT_TSV
#
# Each GDELT archive expands to one daily file, so grouping each file separately
# avoids a single machine-wide sort while still running the submitted mapper and
# reducer over every source record.  The final output has the same one-row-per-day
# format that Stage 2 reads back from HDFS.
set -euo pipefail

INPUT=${1:?usage: $0 INPUT_DIR OUTPUT_TSV}
OUTPUT=${2:?usage: $0 INPUT_DIR OUTPUT_TSV}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$(dirname "$OUTPUT")"

shopt -s nullglob
FILES=("$INPUT"/*.export.CSV)
(( ${#FILES[@]} > 0 )) || { echo "no GDELT files in $INPUT" >&2; exit 1; }

for file in "${FILES[@]}"; do
    name=$(basename "$file")
    python3 "$ROOT/src/streaming/gdelt_mapper.py" < "$file" \
        | LC_ALL=C sort -k1,1 \
        | python3 "$ROOT/src/streaming/gdelt_reducer.py" > "$TMP/$name.tsv"
done

# Each normal DATEADDED key belongs to one daily source file. BAD_* counters can
# occur in any file and are combined here to match a full-directory Streaming run.
awk -F '\t' '
  /^BAD_/ { bad[$1] += $2; next }
  { print }
  END { for (key in bad) printf "%s\\t%d\\tNA\\t0\\n", key, bad[key] }
' "$TMP"/*.tsv | LC_ALL=C sort -k1,1 > "$OUTPUT"

echo "wrote $(wc -l < "$OUTPUT") audit rows to $OUTPUT"
