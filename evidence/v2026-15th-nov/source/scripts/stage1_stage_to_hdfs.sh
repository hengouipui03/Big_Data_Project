#!/usr/bin/env bash
# Stage 1: copy the raw corpus from S3 into HDFS, verify it, and write dated evidence.
#
# Run ON THE EMR PRIMARY NODE (after ssh), from the repo folder:
#     bash scripts/stage1_stage_to_hdfs.sh s3://YOUR-BUCKET/raw /raw evidence/emr
#
# Arguments: 1 = S3 folder holding raw/<source>/<version>/..., 2 = HDFS destination (default /raw),
#            3 = local folder for evidence files (default evidence/emr)
#
# Produces in the evidence folder:
#   stage1_cluster.txt          cluster id, EMR release, node list, time stamps, staging duration
#   stage1_distcp.log           full log of the copy job (contains the YARN application id)
#   stage1_hdfs_listing.txt     hdfs dfs -ls -R of the staged corpus
#   stage1_hdfs_inventory.csv   per-file bytes, line count and sha256 computed FROM HDFS
#                               (compare with evidence/local/inventory.csv)
#
# Staging time is reported separately because the brief excludes it from the 60-minute run.
set -euo pipefail
SRC=${1:?usage: $0 s3://bucket/raw [/hdfs/dest] [evidence_dir]}
DST=${2:-/raw}
EV=${3:-evidence/emr}
mkdir -p "$EV"

CLUSTER_ID=$(jq -r .jobFlowId /mnt/var/lib/info/job-flow.json 2>/dev/null || echo unknown)
RELEASE=$(grep -o 'emr-[0-9.]*' /mnt/var/lib/info/extraInstanceData.json 2>/dev/null | head -1 || echo unknown)

echo "== Stage 1: staging $SRC -> hdfs://$DST on cluster $CLUSTER_ID ($RELEASE)"
START_UTC=$(date -u +%FT%TZ); t0=$(date +%s)

# the copy itself (a MapReduce job; its application id appears in the log)
hadoop distcp "$SRC" "$DST" 2>&1 | tee "$EV/stage1_distcp.log"

t1=$(date +%s); END_UTC=$(date -u +%FT%TZ)
APP_ID=$(grep -o 'application_[0-9_]*' "$EV/stage1_distcp.log" | head -1 || echo unknown)

{
  echo "cluster_id=$CLUSTER_ID"
  echo "emr_release=$RELEASE"
  echo "s3_source=$SRC"
  echo "hdfs_destination=$DST"
  echo "distcp_application_id=$APP_ID"
  echo "staging_start_utc=$START_UTC"
  echo "staging_end_utc=$END_UTC"
  echo "staging_seconds=$((t1 - t0))"
  echo "hdfs_replication=$(hdfs getconf -confKey dfs.replication)"
  echo "--- yarn nodes ---"
  yarn node -list -all 2>/dev/null || true
} | tee "$EV/stage1_cluster.txt"

echo "== HDFS listing"
hdfs dfs -ls -R "$DST" | tee "$EV/stage1_hdfs_listing.txt"
hdfs dfs -du -h "$DST" | tee -a "$EV/stage1_hdfs_listing.txt"

echo "== HDFS capacity and per-worker free space (brief: measure peak use and free space on each worker)"
hdfs dfsadmin -report | tee "$EV/stage1_dfs_report.txt" >/dev/null
head -12 "$EV/stage1_dfs_report.txt"

echo "== Verifying every staged file (bytes, lines, sha256 read back from HDFS)"
echo "hdfs_path,bytes,lines,sha256" > "$EV/stage1_hdfs_inventory.csv"
hdfs dfs -ls -R "$DST" | awk '$1 !~ /^d/ {print $NF}' | while read -r f; do
  bytes=$(hdfs dfs -du "$f" | awk '{print $1}')
  lines=$(hdfs dfs -cat "$f" | wc -l)
  sum=$(hdfs dfs -cat "$f" | sha256sum | cut -d' ' -f1)
  echo "$f,$bytes,$lines,$sum" | tee -a "$EV/stage1_hdfs_inventory.csv"
done

echo "== done. Staging took $((t1 - t0)) s. Evidence written to $EV/"
echo "   Compare sha256 values in $EV/stage1_hdfs_inventory.csv with evidence/local/inventory.csv:"
echo "   identical values prove the corpus in HDFS is byte-for-byte the downloaded source."
