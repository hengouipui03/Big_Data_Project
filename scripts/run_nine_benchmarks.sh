#!/usr/bin/env bash
# Run from the project root on the EMR primary. NEVER run concurrently.
set -Eeuo pipefail
BUCKET=${BENCH_BUCKET:-inf2006-team07-bigdata-1-310947418945-us-east-1-an}
CLUSTER_ID=${BENCH_CLUSTER_ID:?Set BENCH_CLUSTER_ID to the EMR cluster ID}
DEST="s3://$BUCKET/benchmark_runs/18th-nov"
HROOT=/benchmark_runs/18th-nov
ROOT=$(pwd)
WORK="$HOME/inf2006-benchmark-18th-nov"
MODE=${BENCH_MODE:-full}
ATTEMPT=${BENCH_ATTEMPT:-}
case "$MODE" in
  pilot) DEST="s3://$BUCKET/benchmark_pilots/18th-nov"; HROOT=/benchmark_pilots/18th-nov; WORK="$HOME/inf2006-benchmark-pilot-18th-nov"; DB_PREFIX=bench_pilot ;;
  full) DB_PREFIX=bench_run ;;
  *) echo 'BENCH_MODE must be pilot or full' >&2; exit 1 ;;
esac
if [ -n "$ATTEMPT" ]; then
  [[ "$MODE" = full && "$ATTEMPT" =~ ^attempt-[0-9][0-9]$ ]] || { echo 'Invalid BENCH_ATTEMPT' >&2; exit 1; }
  DEST="$DEST/$ATTEMPT"; HROOT="$HROOT/$ATTEMPT"; WORK="$WORK-$ATTEMPT"
fi
INPUT_ROOT=$HROOT
if [ "${BENCH_REUSE_INPUTS:-0}" = 1 ]; then
  [ -n "$ATTEMPT" ] || { echo 'Input reuse requires a separate attempt.' >&2; exit 1; }
  INPUT_ROOT=/benchmark_runs/18th-nov
fi
mkdir -p "$WORK"
command -v hadoop >/dev/null
command -v spark-submit >/dev/null
command -v hive >/dev/null
[ -f notebooks/cells/01_setup.cells ] && [ -f evidence/local/q1_2016/inventory.csv ] || { echo 'Run from the full project root.' >&2; exit 1; }
nodes=$(hdfs dfsadmin -report | sed -n 's/^Live datanodes (\([0-9]*\)):.*/\1/p')
[ "$nodes" = 2 ] || { echo "Expected 2 live HDFS data nodes, found $nodes" >&2; exit 1; }
[ "$(hdfs getconf -confKey dfs.replication)" = 2 ] || { echo 'Expected HDFS replication 2' >&2; exit 1; }
if hdfs dfs -test -e "$HROOT" || [ -n "$(aws s3 ls "$DEST/" --recursive)" ]; then
  echo 'Benchmark destination already exists: refusing to overwrite evidence.' >&2; exit 1
fi
existing_dbs=$(hive -S -e "SHOW DATABASES LIKE '${DB_PREFIX}*'" 2> "$WORK/hive_preflight.log")
if printf '%s\n' "$existing_dbs" | grep -Eq "^${DB_PREFIX}[0-9][0-9]$"; then
  echo 'A benchmark Hive database already exists: refusing to drop its tables.' >&2; exit 1
fi
for n in 01 02 03 04 05 06 07 08 09; do
  if hdfs dfs -test -e "/user/hive/warehouse/$DB_PREFIX$n.db"; then
    echo "Hive warehouse path $DB_PREFIX$n already exists; refusing to overwrite." >&2; exit 1
  fi
done
if [ -n "$(yarn application -list -appStates RUNNING,ACCEPTED 2>/dev/null | awk '/application_[0-9]+_[0-9]+/ {print $1}')" ]; then
  echo 'Active YARN applications detected; stop them before the benchmark.' >&2; exit 1
fi
mapfile -t WORKERS < <(yarn node -list -all 2>/dev/null | awk '$2 == "RUNNING" {sub(/:[0-9]+$/, "", $1); print $1}')
[ "${#WORKERS[@]}" = 2 ] || { echo 'Expected two running YARN workers.' >&2; exit 1; }
for worker in "${WORKERS[@]}"; do
  ssh -o BatchMode=yes -o ConnectTimeout=8 -o StrictHostKeyChecking=accept-new "hadoop@$worker" 'hostname; df -Pk' >/dev/null || {
    echo "Cannot sample disk on $worker using SSH. Configure worker SSH before benchmarking." >&2; exit 1;
  }
done
aws s3 ls "s3://$BUCKET/dependencies/spark-numpy-env.tar.gz" >/dev/null
DRIVER_PYTHON=${BENCH_DRIVER_PYTHON:-"$HOME/inf2006-benchmark-driver-env/bin/python"}
[ -x "$DRIVER_PYTHON" ] || { echo "Driver Python missing: $DRIVER_PYTHON" >&2; exit 1; }
"$DRIVER_PYTHON" -c 'import sys,numpy; print("Driver Python:",sys.version,"NumPy:",numpy.__version__)' || { echo 'Driver cannot import NumPy.' >&2; exit 1; }
mkdir -p "$WORK/setup"
aws emr describe-cluster --cluster-id "$CLUSTER_ID" > "$WORK/setup/cluster.json"
aws emr list-instances --cluster-id "$CLUSTER_ID" > "$WORK/setup/instances.json"
cp evidence/local/inventory.csv "$WORK/setup/development_inventory.csv"
cp evidence/local/q1_2016/inventory.csv "$WORK/setup/full_inventory.csv"
cp -R notebooks/cells scripts sql src "$WORK/setup/"
hdfs dfsadmin -report > "$WORK/setup/hdfs_before.txt"
yarn node -list -all > "$WORK/setup/yarn_nodes.txt" 2>&1
date -u +%FT%TZ > "$WORK/setup/staging_start_utc.txt"
if [ "${BENCH_REUSE_INPUTS:-0}" != 1 ]; then
  hdfs dfs -mkdir -p "$INPUT_ROOT/inputs/full" "$INPUT_ROOT/inputs/dev"
  hadoop distcp "s3://$BUCKET/raw/prices/" "hdfs://$INPUT_ROOT/inputs/full/prices"
  hadoop distcp "s3://$BUCKET/raw/gdelt/" "hdfs://$INPUT_ROOT/inputs/full/gdelt"
  hadoop distcp "s3://$BUCKET/raw/gdelt_development_pfp/" "hdfs://$INPUT_ROOT/inputs/dev/gdelt"
else
  printf '%s\n' "Reusing staged input files from $INPUT_ROOT; all files are reverified." > "$WORK/setup/staging_reuse.txt"
fi
date -u +%FT%TZ > "$WORK/setup/staging_end_utc.txt"
# Verify every staged byte against the frozen project inventories, outside timing.
python3 scripts/verify_benchmark_inputs.py "$INPUT_ROOT" > "$WORK/setup/input_verification.json"
aws s3 sync "$WORK/setup/" "$DEST/setup/"

run_one() {
  local id=$1 db=$2 config=$3 profile=$4 audit=$5 raw_gdelt=$6
  local d="$WORK/$id" start end status=0
  mkdir -p "$d/project/sql/hive" "$d/project/src/streaming" "$d/project/scripts" "$d/evidence"
  cp scripts/stage2_streaming_and_hive.sh "$d/project/scripts/"
  cp src/streaming/gdelt_mapper.py src/streaming/gdelt_reducer.py "$d/project/src/streaming/"
  for f in sql/hive/*.hql; do sed "s/inf2006/$db/g" "$f" > "$d/project/$f"; done
  printf '%s\n' "$id" "$db" "$config" "$profile" > "$d/run_identity.txt"
  # Capture filesystem use on each worker every 10 seconds. Failed samples remain in the log.
  (
    while :; do
      for worker in "${WORKERS[@]}"; do
        printf '\nSAMPLE %s %s\n' "$(date -u +%FT%TZ)" "$worker"
        ssh -o BatchMode=yes -o ConnectTimeout=8 "hadoop@$worker" 'df -Pk; cat /proc/meminfo | head -5' 2>&1 || echo 'SAMPLE FAILED'
      done
      sleep 10
    done
  ) > "$d/worker_resource_samples.txt" 2>&1 &
  local sampler=$!
  start=$(date +%s); date -u +%FT%TZ > "$d/start_utc.txt"
  (
    cd "$d/project"
    export BENCH_RAW_PRICES="$INPUT_ROOT/inputs/full/prices"
    export BENCH_RAW_GDELT="$raw_gdelt"
    export BENCH_AUDIT_OUT="$HROOT/$id/streaming_audit"
    bash scripts/stage2_streaming_and_hive.sh "$id" "$d/evidence" 2 "$ROOT/$audit"
  ) > "$d/stage2_console.log" 2>&1 || status=$?
  if [ "$status" = 0 ]; then
    spark-submit --master yarn --deploy-mode client \
      --conf spark.executor.instances=2 --conf spark.executor.cores=2 --conf spark.executor.memory=6g \
      --conf spark.executor.memoryOverhead=2g --conf spark.driver.memory=2g \
      --conf spark.dynamicAllocation.enabled=false --conf spark.sql.catalogImplementation=hive \
      --conf spark.pyspark.python=./environment/bin/python --conf "spark.pyspark.driver.python=$DRIVER_PYTHON" \
      --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python \
      --archives "s3://$BUCKET/dependencies/spark-numpy-env.tar.gz#environment" \
      "$ROOT/scripts/benchmark_spark_run.py" "$id" "$db" "$config" "$BUCKET" "$ROOT" "$HROOT" \
      > "$d/spark_console.log" 2>&1 || status=$?
  fi
  end=$(date +%s); date -u +%FT%TZ > "$d/end_utc.txt"
  kill "$sampler" 2>/dev/null || true
  wait "$sampler" 2>/dev/null || true
  # Preservation is outside the measured period; preserve Hive data and Streaming output.
  hdfs dfs -get "$HROOT/$id/streaming_audit" "$d/streaming_audit" || status=1
  hdfs dfs -get "/user/hive/warehouse/$db.db" "$d/hive_tables" || status=1
  hdfs dfs -get "$HROOT/$id/prepared" "$d/prepared" || status=1
  aws s3 cp "$DEST/$id/spark/evidence/$config/benchmark_spark_record.json" "$d/benchmark_spark_record.json" || status=1
  if grep -q 'SAMPLE FAILED' "$d/worker_resource_samples.txt"; then status=1; fi
  mkdir -p "$d/yarn_logs"
  grep -ohE 'application_[0-9]+_[0-9]+' "$d/stage2_console.log" "$d/spark_console.log" 2>/dev/null | sort -u > "$d/application_ids.txt" || true
  while read -r app; do
    [ -n "$app" ] || continue
    yarn logs -applicationId "$app" > "$d/yarn_logs/$app.log" 2> "$d/yarn_logs/$app.status" || true
  done < "$d/application_ids.txt"
  hdfs dfsadmin -report > "$d/hdfs_postrun.txt" 2>&1 || true
  printf '{"run_id":"%s","profile":"%s","config":"%s","seconds":%s,"exit_status":%s}\n' "$id" "$profile" "$config" "$((end-start))" "$status" > "$d/timing.json"
  aws s3 sync "$d/" "$DEST/$id/records/"
  [ "$status" = 0 ] || { echo "Run $id failed: preserved at $DEST/$id/records/" >&2; return "$status"; }
  echo "Completed $id in $((end-start)) seconds"
}
if [ "$MODE" = pilot ]; then
  run_one run01-reference-fullprofile bench_pilot01 reference full evidence/local/q1_2016/streaming_gdelt_audit.tsv "$INPUT_ROOT/inputs/full/gdelt"
  echo "Pilot finished. Inspect its evidence before any assessed runs."
  exit 0
fi
run_one run01-reference-fullprofile bench_run01 reference full evidence/local/q1_2016/streaming_gdelt_audit.tsv "$INPUT_ROOT/inputs/full/gdelt"
run_one run02-reference-fullprofile bench_run02 reference full evidence/local/q1_2016/streaming_gdelt_audit.tsv "$INPUT_ROOT/inputs/full/gdelt"
run_one run03-reference-fullprofile bench_run03 reference full evidence/local/q1_2016/streaming_gdelt_audit.tsv "$INPUT_ROOT/inputs/full/gdelt"
run_one run04-reference-7d bench_run04 reference 7d evidence/local/streaming_gdelt_audit.tsv "$INPUT_ROOT/inputs/dev/gdelt"
run_one run05-reference-7d bench_run05 reference 7d evidence/local/streaming_gdelt_audit.tsv "$INPUT_ROOT/inputs/dev/gdelt"
run_one run06-reference-7d bench_run06 reference 7d evidence/local/streaming_gdelt_audit.tsv "$INPUT_ROOT/inputs/dev/gdelt"
run_one run07-alternative-fullprofile bench_run07 alternative full evidence/local/q1_2016/streaming_gdelt_audit.tsv "$INPUT_ROOT/inputs/full/gdelt"
run_one run08-alternative-fullprofile bench_run08 alternative full evidence/local/q1_2016/streaming_gdelt_audit.tsv "$INPUT_ROOT/inputs/full/gdelt"
run_one run09-alternative-fullprofile bench_run09 alternative full evidence/local/q1_2016/streaming_gdelt_audit.tsv "$INPUT_ROOT/inputs/full/gdelt"
python3 scripts/summarize_benchmark.py "$WORK" > "$WORK/summary.json"
aws s3 cp "$WORK/summary.json" "$DEST/summary.json"
