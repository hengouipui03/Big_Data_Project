"""Execute the project's Spark calculation cells for one isolated EMR benchmark.

Called by the benchmark launcher with spark-submit, not from Jupyter. The original
Sparkmagic notebook is retained for interactive use and fresh-session verification.
"""
import json
import os
import pathlib
import re
import sys
import urllib.request

from pyspark.sql import SparkSession


run_id, database, config, bucket, project, hroot = sys.argv[1:7]
if not re.fullmatch(r"run\d\d-(?:reference|alternative)-(?:fullprofile|7d)", run_id):
    raise ValueError("invalid run identifier")
if not re.fullmatch(r"bench_(?:run|pilot)\d\d", database):
    raise ValueError("invalid Hive database")
if hroot not in ("/benchmark_runs/18th-nov", "/benchmark_pilots/18th-nov"):
    raise ValueError("invalid benchmark root")
if config not in ("reference", "alternative"):
    raise ValueError("invalid configuration")

spark = SparkSession.builder.enableHiveSupport().appName(run_id).getOrCreate()
spark.sparkContext.setLogLevel("WARN")
g = {"spark": spark, "__name__": "__main__"}

def code_cells(path):
    kind, lines = None, []
    for line in pathlib.Path(path).read_text(encoding="utf-8").splitlines():
        if line.startswith("#@@ "):
            if kind == "code":
                yield "\n".join(lines)
            kind, lines = line[4:].strip(), []
        else:
            lines.append(line)
    if kind == "code":
        yield "\n".join(lines)

try:
    for filename in ("01_setup.cells", "02_prepare.cells", "03_models.cells", "04_analytics.cells"):
        for index, source in enumerate(code_cells(pathlib.Path(project) / "notebooks" / "cells" / filename)):
            # The remaining cells are Sparkmagic/local plotting, not distributed analytics.
            if source.lstrip().startswith("%%"):
                if filename == "04_analytics.cells":
                    break
                continue
            source = source.replace("inf2006.", database + ".").replace("SHOW TABLES IN inf2006", "SHOW TABLES IN " + database)
            if re.search(r"^VERSION\s*=", source, re.M):
                for key, value in (("VERSION", run_id), ("BUCKET", bucket), ("ACTIVE_CONFIG", config)):
                    source = re.sub(rf"^{key}\s*=.*$", f"{key} = {value!r}", source, count=1, flags=re.M)
                source = source.replace('f"hdfs:///prepared/{VERSION}/{ACTIVE_CONFIG}"', f'f"hdfs://{hroot}/{{VERSION}}/prepared"')
                source = source.replace('f"hdfs:///models/{VERSION}"', f'f"hdfs://{hroot}/{{VERSION}}/models"')
                source = source.replace('f"s3://{BUCKET}/notebook_outputs/{VERSION}"', f'f"s3://{{BUCKET}}{hroot}/{{VERSION}}/spark"')
                source = source.replace('f"hdfs:///project/notebook_outputs/{VERSION}"', f'f"s3://{{BUCKET}}{hroot}/{{VERSION}}/spark"')
            print(f"BENCHMARK CELL {filename}:{index}", flush=True)
            exec(compile(source, f"{filename}:{index}", "exec"), g)

    # Persist execution evidence before the application ends. The UI endpoint can
    # be unavailable on some EMR versions, so preserve the error explicitly.
    try:
        url = f"{spark.sparkContext.uiWebUrl}/api/v1/applications/{spark.sparkContext.applicationId}/executors"
        with urllib.request.urlopen(url, timeout=10) as response:
            memory = json.load(response)
    except Exception as exc:
        memory = {"unavailable": str(exc)}
    record = {"run_id": run_id, "database": database, "config": config,
              "application_id": spark.sparkContext.applicationId,
              "stages": g["CURRENT_LOG"], "checks": g["CHECKS"], "executor_metrics": memory,
              "gdelt_summary": g["gd_summary"], "prepared": g["PREPARED"]}
    g["save_json"]("benchmark_spark_record.json", record)
    if any(not c["passed"] for c in g["CHECKS"]):
        raise RuntimeError("one or more notebook checks failed; see benchmark_spark_record.json")
    if not isinstance(memory, list) or not any(e.get("peakMemoryMetrics") for e in memory):
        raise RuntimeError("Spark executor peak memory metrics were unavailable; see benchmark_spark_record.json")
finally:
    spark.stop()
