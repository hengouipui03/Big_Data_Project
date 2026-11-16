#!/usr/bin/env bash
# Acquire GDELT 1.0 daily event files for a date range: download, verify md5, unzip, load, record provenance.
# Runs unchanged on the EMR primary node (DEST = hdfs path) or locally (DEST = directory).
#
# Usage: scripts/acquire_gdelt.sh START END DEST [JOBS] [MANIFEST_DIR]
#   START, END   inclusive dates, YYYYMMDD            e.g. 20150701 20170630
#   DEST         hdfs:///data/raw/gdelt  |  local directory
#   JOBS         parallel downloads (default 8)
#   MANIFEST_DIR where per-day provenance records go (default evidence/gdelt_manifest)
# Resumable: days already present in DEST are skipped. A day is only kept if its md5 matches the published index.
set -uo pipefail
START=${1:?start YYYYMMDD}; END=${2:?end YYYYMMDD}; DEST=${3:?dest}; JOBS=${4:-8}
MANIFEST_DIR=${5:-evidence/gdelt_manifest}
BASE="https://data.gdeltproject.org/events"
TMP=${TMPDIR:-/tmp}/gdelt_acquire.$$
mkdir -p "$MANIFEST_DIR" "$TMP"
trap 'rm -rf "$TMP"' EXIT

is_hdfs() { [[ "$DEST" == hdfs://* || "$DEST" == /user/* || "$DEST" == /data/* ]]; }
exists() {  # is the extracted day already at DEST?
  if is_hdfs; then hdfs dfs -test -e "$DEST/$1.export.CSV"; else [[ -s "$DEST/$1.export.CSV" ]]; fi
}

curl -sf --retry 5 "$BASE/md5sums" -o "$TMP/md5sums" || { echo "cannot fetch md5sums index" >&2; exit 1; }
cp "$TMP/md5sums" "$MANIFEST_DIR/md5sums.index.txt"      # keep the exact index used
is_hdfs || mkdir -p "$DEST"
is_hdfs && hdfs dfs -mkdir -p "$DEST"

fetch_day() {
  d=$1; f="$d.export.CSV.zip"; rec="$MANIFEST_DIR/$d.tsv"
  if exists "$d" && [[ -s "$rec" ]]; then echo "skip $d"; return 0; fi
  want=$(awk -v f="$f" '$2==f {print $1}' "$TMP/md5sums")
  [[ -z "$want" ]] && { printf '%s\t%s\t\t\t\tMISSING_IN_INDEX\t%s\n' "$d" "$BASE/$f" "$(date -u +%FT%TZ)" > "$rec"; echo "no index entry $d"; return 1; }
  for attempt in 1 2 3; do
    curl -sf --retry 3 "$BASE/$f" -o "$TMP/$f" || continue
    got=$(md5sum "$TMP/$f" | awk '{print $1}')
    [[ "$got" == "$want" ]] && break
    got="MISMATCH:$got"
  done
  size=$(stat -c %s "$TMP/$f" 2>/dev/null || echo 0)
  if [[ "${got:-}" != "$want" ]]; then
    printf '%s\t%s\t%s\t%s\t%s\tFAILED\t%s\n' "$d" "$BASE/$f" "$size" "$want" "${got:-none}" "$(date -u +%FT%TZ)" > "$rec"
    rm -f "$TMP/$f"; echo "FAILED $d"; return 1
  fi
  if is_hdfs; then unzip -p "$TMP/$f" | hdfs dfs -put -f - "$DEST/$d.export.CSV"
  else unzip -p "$TMP/$f" > "$DEST/$d.export.CSV"; fi
  rc=$?
  rm -f "$TMP/$f"
  status=OK; [[ $rc -ne 0 ]] && status=LOAD_FAILED
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$d" "$BASE/$f" "$size" "$want" "$got" "$status" "$(date -u +%FT%TZ)" > "$rec"
  echo "$status $d"
}
export -f fetch_day exists is_hdfs
export BASE DEST TMP MANIFEST_DIR

dates=(); cur=$(date -u -d "$START" +%Y%m%d); last=$(date -u -d "$END" +%Y%m%d)
while [[ "$cur" -le "$last" ]]; do dates+=("$cur"); cur=$(date -u -d "$cur + 1 day" +%Y%m%d); done
echo "acquiring ${#dates[@]} days $START..$END into $DEST with $JOBS parallel jobs"
printf '%s\n' "${dates[@]}" | xargs -P "$JOBS" -I{} bash -c 'fetch_day {}'

# merge per-day records into one manifest
{ printf 'date\turl\tzip_bytes\tmd5_published\tmd5_actual\tstatus\tacquired_utc\n'
  cat "$MANIFEST_DIR"/[0-9]*.tsv | sort; } > "$MANIFEST_DIR/manifest.tsv"
echo "manifest: $MANIFEST_DIR/manifest.tsv  (OK: $(grep -c $'\tOK\t' "$MANIFEST_DIR/manifest.tsv") of ${#dates[@]})"
