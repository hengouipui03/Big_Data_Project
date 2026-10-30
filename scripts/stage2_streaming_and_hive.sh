#!/usr/bin/env bash
# Stage 2: Hadoop Streaming audit job + Hive registration and SQL transforms, with evidence.
#
# Run ON THE EMR PRIMARY NODE from the repo folder (after Stage 1 has staged /raw):
#     bash scripts/stage2_streaming_and_hive.sh v2026-10-27 evidence/emr_ref_m5xlarge
#
# Arguments: 1 = raw version folder name under /raw/<source>/ (default v2026-10-27)
#            2 = evidence folder (default evidence/emr_ref_m5xlarge)
#            3 = number of reducers for the streaming job (default 2)
#            4 = matching local streaming-audit file (default development profile)
# Resume:    FROM=c bash scripts/stage2_streaming_and_hive.sh ...   starts at sub-stage c
#            (a = streaming, b = Hive register, c = Hive transform, d = Hive results)
#
# Produces in the evidence folder:
#   stage2_streaming.log           full Hadoop Streaming job log (application id, counters)
#   stage2_streaming_output.tsv    the audit table read back from HDFS
#   stage2_streaming_vs_local.txt  per-day comparison with evidence/local/streaming_gdelt_audit.tsv
#   stage2_hive_01_register.log    Hive output: database/tables created, DESCRIBE FORMATTED
#   stage2_hive_02_transform.log   Hive output: Tez application id(s), DAG progress
#   stage2_hive_03_results.log     Hive query results (quality audit, coverage, row counts)
#   run_log.csv                    one line per stage: engine, start/end UTC, seconds, app ids, in/out
set -euo pipefail
VER=${1:-v2026-10-27}
EV=${2:-evidence/emr_ref_m5xlarge}
REDUCERS=${3:-2}
LOCAL_AUDIT=${4:-evidence/local/streaming_gdelt_audit.tsv}
mkdir -p "$EV"

RAW_GDELT=/raw/gdelt/$VER
RAW_PRICES=/raw/prices/$VER
AUDIT_OUT=/audit/gdelt_daily/$VER
STREAMING_JAR=$(ls /usr/lib/hadoop/hadoop-streaming.jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar 2>/dev/null | head -1)
HIVE_CMD=${HIVE_CMD:-hive}
FROM=${FROM:-a}
runstep() { [[ "$1" < "$FROM" ]] && return 1 || return 0; }
RUNLOG="$EV/run_log.csv"
[ -f "$RUNLOG" ] || echo "stage,engine,start_utc,end_utc,seconds,application_ids,input,output,rows_out,notes" > "$RUNLOG"

now() { date -u +%FT%TZ; }
appids() { grep -o 'application_[0-9_]*' "$1" | sort -u | tr '\n' ' ' | sed 's/ $//'; }
logrow() { echo "$1,$2,$3,$4,$5,\"$6\",$7,$8,$9,\"${10}\"" >> "$RUNLOG"; }

if runstep a; then
echo "== Stage 2a: Hadoop Streaming audit over $RAW_GDELT -> $AUDIT_OUT ($REDUCERS reducers)"
hdfs dfs -rm -r -f "$AUDIT_OUT" >/dev/null 2>&1 || true
S=$(now); t0=$(date +%s)
hadoop jar "$STREAMING_JAR" \
  -D mapreduce.job.name="inf2006_gdelt_daily_audit_$VER" \
  -D mapreduce.job.reduces="$REDUCERS" \
  -files src/streaming/gdelt_mapper.py,src/streaming/gdelt_reducer.py \
  -mapper "python3 gdelt_mapper.py" \
  -reducer "python3 gdelt_reducer.py" \
  -input "$RAW_GDELT" \
  -output "$AUDIT_OUT" 2>&1 | tee "$EV/stage2_streaming.log"
t1=$(date +%s); E=$(now)
hdfs dfs -cat "$AUDIT_OUT/part-*" | sort > "$EV/stage2_streaming_output.tsv"
ROWS=$(wc -l < "$EV/stage2_streaming_output.tsv")
logrow stage2a_streaming_audit hadoop_streaming "$S" "$E" $((t1-t0)) "$(appids "$EV/stage2_streaming.log")" "$RAW_GDELT" "$AUDIT_OUT" "$ROWS" "mapper=gdelt_mapper.py reducer=gdelt_reducer.py reducers=$REDUCERS"
echo "   streaming took $((t1-t0)) s; $ROWS output rows"

echo "== Stage 2a check: EMR streaming output vs local (laptop) run"
python3 - "$EV/stage2_streaming_output.tsv" "$LOCAL_AUDIT" <<'EOF' | tee "$EV/stage2_streaming_vs_local.txt"
import sys
def load(p):
    d = {}
    for line in open(p):
        k, n, tone, lag = line.rstrip("\n").split("\t")
        d[k] = (int(n), tone, int(lag))
    return d
emr, loc = load(sys.argv[1]), load(sys.argv[2])
ok = True
print(f"{'key':14} {'emr_events':>11} {'local_events':>13} {'emr_tone':>9} {'local_tone':>11} {'emr_lag':>8} {'local_lag':>10} match")
for k in sorted(set(emr) | set(loc)):
    e, l = emr.get(k), loc.get(k)
    if k.startswith("BAD_"):   # malformed-line counters: report, do not compare
        print(f"{k:14} {e[0] if e else '-':>11} {l[0] if l else '-':>13} {'':>9} {'':>11} {'':>8} {'':>10} info")
        continue
    m = e is not None and l is not None and e[0] == l[0] and e[2] == l[2] and abs(float(e[1]) - float(l[1])) < 1e-3
    ok &= m
    print(f"{k:14} {e[0] if e else '-':>11} {l[0] if l else '-':>13} {e[1] if e else '-':>9} {l[1] if l else '-':>11} {e[2] if e else '-':>8} {l[2] if l else '-':>10} {'yes' if m else 'NO'}")
print("RESULT:", "EMR streaming output matches local run on every date key" if ok else "MISMATCH - investigate")
EOF
fi

if runstep b; then
echo "== Stage 2b: Hive register raw tables"
S=$(now); t0=$(date +%s)
$HIVE_CMD --hivevar raw_prices_path="$RAW_PRICES" --hivevar audit_path="$AUDIT_OUT" \
  -f sql/hive/01_register_raw.hql 2>&1 | tee "$EV/stage2_hive_01_register.log"
t1=$(date +%s); E=$(now)
logrow stage2b_hive_register hive_tez "$S" "$E" $((t1-t0)) "$(appids "$EV/stage2_hive_01_register.log")" "$RAW_PRICES;$AUDIT_OUT" "inf2006.raw_prices;inf2006.gdelt_daily_audit" "" "external tables, no data movement"
fi

if runstep c; then
echo "== Stage 2c: Hive quality audit + typed tables (Tez)"
S=$(now); t0=$(date +%s)
$HIVE_CMD -f sql/hive/02_quality_and_typed.hql 2>&1 | tee "$EV/stage2_hive_02_transform.log"
t1=$(date +%s); E=$(now)
logrow stage2c_hive_transform hive_tez "$S" "$E" $((t1-t0)) "$(appids "$EV/stage2_hive_02_transform.log")" "inf2006.raw_prices;inf2006.gdelt_daily_audit" "inf2006.prices_typed;gdelt_daily_typed;raw_prices_quality;raw_prices_duplicates;raw_prices_series_length;gdelt_audit_rejects;prices_yearly_coverage" "" "CTAS on Tez"
fi

if runstep d; then
echo "== Stage 2d: Hive results"
S=$(now); t0=$(date +%s)
$HIVE_CMD -f sql/hive/03_show_results.hql 2>&1 | tee "$EV/stage2_hive_03_results.log"
t1=$(date +%s); E=$(now)
logrow stage2d_hive_results hive_tez "$S" "$E" $((t1-t0)) "$(appids "$EV/stage2_hive_03_results.log")" "inf2006.*" "stdout" "" "SELECT evidence"
fi

echo "== Hive warehouse contents"
hdfs dfs -ls -R /user/hive/warehouse/inf2006.db 2>/dev/null | tee "$EV/stage2_hive_warehouse_listing.txt" || true

echo "== Stage 2 done. Evidence in $EV/ ; run log:"
cat "$RUNLOG"
echo
echo "   Expected in stage2_hive_03_results.log: rows_total=619040 tickers=505 rows_up_to_2017_12_29=605910"
echo "   empty_open=11 empty_high=8 empty_low=8 ohlc_inconsistent=12 nonpositive=0 duplicate_ticker_date=0"
echo "   (these reconcile with evidence/local/quality_summary.json)"
