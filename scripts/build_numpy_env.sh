#!/usr/bin/env bash
# Build the Python environment that the PySpark notebook ships to the YARN executors, and upload it to S3.
#
# WHY: on emr-7.10.0 the system Python 3.9 has no numpy, but pyspark.ml needs it on the Python side
# (vectors, evaluators, collecting ML columns). The first cell of notebooks/inf2006_pipeline.ipynb therefore sets
#     spark.yarn.dist.archives      = s3://<bucket>/dependencies/spark-numpy-env.tar.gz#environment
#     spark.pyspark.python          = ./environment/bin/python
#     spark.executorEnv.PYSPARK_PYTHON = ./environment/bin/python
# so every executor unpacks this archive and uses its interpreter.
#
# Run ONCE per bucket, ON THE EMR PRIMARY NODE (same OS image / Python 3.9 as the workers), before opening the notebook:
#     bash scripts/build_numpy_env.sh <bucket-name>
# Needs outbound internet access (pip) and write access to the bucket (the EMR instance role has it in AWS Academy).
# Takes about 2 minutes. Never put credentials in this file.
#
# STATUS: this script reproduces the archive used for the 2026-11-04 / 2026-11-13 runs (named spark-numpy-env.tar.gz,
# Python 3.9.25). The original archive was built by hand; re-run and verify this script during the next cluster rehearsal.
set -euo pipefail
BUCKET=${1:?usage: bash scripts/build_numpy_env.sh <bucket-name (no s3:// prefix)>}
NUMPY_SPEC=${NUMPY_SPEC:-numpy}          # pin here if you need an exact version, e.g. NUMPY_SPEC="numpy==1.26.4"
WORK=${WORK:-/tmp/spark-numpy-env}
ARCHIVE=/tmp/spark-numpy-env.tar.gz

command -v python3 >/dev/null || { echo "python3 not found - run this on the EMR primary node" >&2; exit 1; }
echo "system python: $(python3 --version)  (the executors must use the same minor version; expected 3.9.x)"

rm -rf "$WORK" "$ARCHIVE"
python3 -m venv --copies "$WORK"
"$WORK/bin/python" -m pip install --quiet --upgrade pip
"$WORK/bin/python" -m pip install --quiet "$NUMPY_SPEC"
"$WORK/bin/python" - <<'PY'
import sys, numpy
print("env python:", sys.version.split()[0], "| numpy", numpy.__version__)
PY

# relative paths inside the archive, so YARN can unpack it under any name ("#environment" in the Spark conf)
tar -czf "$ARCHIVE" -C "$WORK" .
ls -l "$ARCHIVE"
aws s3 cp "$ARCHIVE" "s3://$BUCKET/dependencies/spark-numpy-env.tar.gz" --only-show-errors
aws s3 ls "s3://$BUCKET/dependencies/spark-numpy-env.tar.gz"
echo "OK. Replace the bucket name in the %%configure cell and in cell 0.1 of the notebook with: $BUCKET"
