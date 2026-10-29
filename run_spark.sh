#!/usr/bin/env bash
# Local dev only (NOT the assessed EMR run): run a PySpark script in a Spark 3.5.3 container.
# Usage: ./run_spark.sh src/pipeline/some_script.py [args...]
export MSYS_NO_PATHCONV=1
docker run --rm -v "$(pwd -W):/work" -w /work -e PYTHONPATH=/work/src \
  --user root bigdata-local-spark \
  /opt/spark/bin/spark-submit --master "local[4]" --driver-memory 4g \
  --conf spark.sql.shuffle.partitions=8 --conf spark.sql.warehouse.dir=/work/work/spark-warehouse "$@"
