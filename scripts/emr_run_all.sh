#!/usr/bin/env bash
# One command for everything that must happen on the EMR primary node BEFORE the notebook:
#   0. free the cluster (stop any leftover Spark/Livy session that would block MapReduce/Hive)
#   1. Stage 1: S3 -> HDFS (DistCp) with byte/SHA-256 verification
#   2. Stage 2: Hadoop Streaming audit + Hive on Tez (register, SQL transforms, results)
#   3. publish run_log.csv to HDFS (the notebook reads it for end-to-end timing)
#   4. sanity-check the Hive tables the notebook needs, and save the evidence to S3
#
# Run on the primary node, from the repository folder:
#     bash scripts/emr_run_all.sh inf2006-team07-bigdata v2026-10-27
# Arguments: 1 = S3 bucket name (no s3:// prefix)   2 = raw-data version (default v2026-10-27)
#            3 = evidence folder (default evidence/emr_ref_m5xlarge)
# Re-running is safe: HDFS and Hive outputs are recreated.
set -uo pipefail
BUCKET=${1:?usage: bash scripts/emr_run_all.sh <bucket> [version] [evidence_dir]}
VER=${2:-v2026-10-27}
EV=${3:-evidence/emr_ref_m5xlarge}
case "$VER" in
  *q1-2016*) LOCAL_AUDIT=evidence/local/q1_2016/streaming_gdelt_audit.tsv ;;
  *)         LOCAL_AUDIT=evidence/local/streaming_gdelt_audit.tsv ;;
esac
say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
die() { printf '\n\033[1;31mSTOP: %s\033[0m\n' "$*" >&2; exit 1; }

command -v hadoop >/dev/null || die "hadoop not found - run this ON the EMR primary node (ssh in first)."
[ -f scripts/stage1_stage_to_hdfs.sh ] || die "run from the repository folder (cd Big_Data_Project)."
[ -f "$LOCAL_AUDIT" ] || die "missing $LOCAL_AUDIT (git pull to get the repository evidence files)."
mkdir -p "$EV"

say "0/4 free the cluster: stop leftover Spark/Livy sessions"
LEFT=$(yarn application -list -appStates RUNNING,ACCEPTED 2>/dev/null | awk '/livy|Livy|PySpark|pyspark/ {print $1}')
if [ -n "$LEFT" ]; then
  echo "found running notebook sessions, killing: $LEFT"
  for a in $LEFT; do yarn application -kill "$a" || true; done
else
  echo "no leftover sessions."
fi

say "1/4 Stage 1: S3 -> HDFS  (s3://$BUCKET/raw -> /raw)"
hdfs dfs -test -d "/raw/prices/$VER" && echo "note: /raw/prices/$VER already exists in HDFS; DistCp will update it."
bash scripts/stage1_stage_to_hdfs.sh "s3://$BUCKET/raw" /raw "$EV" || die "Stage 1 failed. See $EV/stage1_distcp.log. Most common cause: wrong bucket name or the files are not under s3://$BUCKET/raw/prices/$VER/ and raw/gdelt/$VER/."
hdfs dfs -test -d "/raw/prices/$VER" || die "/raw/prices/$VER is missing in HDFS: the version folder '$VER' does not exist in s3://$BUCKET/raw/prices/"
hdfs dfs -test -d "/raw/gdelt/$VER"  || die "/raw/gdelt/$VER is missing in HDFS: the version folder '$VER' does not exist in s3://$BUCKET/raw/gdelt/"

say "2/4 Stage 2: Hadoop Streaming + Hive on Tez"
bash scripts/stage2_streaming_and_hive.sh "$VER" "$EV" 2 "$LOCAL_AUDIT" || die "Stage 2 failed. Resume with:  FROM=c bash scripts/stage2_streaming_and_hive.sh $VER $EV 2 $LOCAL_AUDIT   (a=streaming b=register c=transform d=results)"

say "3/4 publish run_log.csv to HDFS for the notebook"
hdfs dfs -mkdir -p "/evidence/$VER"
hdfs dfs -put -f "$EV/run_log.csv" "/evidence/$VER/run_log.csv" && echo "ok: hdfs:///evidence/$VER/run_log.csv"

say "4/4 sanity checks"
N=$(hive -S -e "SELECT COUNT(*) FROM inf2006.prices_typed" 2>/dev/null | tail -1)
D=$(hive -S -e "SELECT COUNT(*) FROM inf2006.gdelt_daily_typed" 2>/dev/null | tail -1)
echo "inf2006.prices_typed rows      : $N   (expected 619040)"
echo "inf2006.gdelt_daily_typed days : $D   (expected 7 for v2026-10-27, 91 for the Q1-2016 profile)"
tail -1 "$EV/stage2_streaming_vs_local.txt"
aws s3 cp "$EV" "s3://$BUCKET/evidence/$(basename "$EV")/" --recursive --only-show-errors && echo "evidence copied to s3://$BUCKET/evidence/$(basename "$EV")/" || echo "WARNING: could not copy evidence to S3 (check the EMR role); copy it by scp instead."

if [ "$N" = "619040" ]; then
  printf '\n\033[1;32mALL GOOD. Now open JupyterHub and run the notebook (docs/RUN_ON_EMR.md, step 8).\033[0m\n'
else
  die "Hive table row count is '$N', expected 619040. Do NOT run the notebook yet; see docs/RUN_ON_EMR.md Troubleshooting."
fi
