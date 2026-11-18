"""Create the JupyterHub PySpark notebook for browsing preserved run outputs."""
import json
from pathlib import Path

def md(text):
    return {"cell_type": "markdown", "metadata": {}, "source": text.splitlines(True)}
def code(text):
    return {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": text.splitlines(True)}

cells = [
    md("# Inspect the 18 November benchmark runs\n\nSelect a run below. The saved Hive table files and evidence are read from that run's S3 folder. The Hive database exists only while the original EMR cluster exists. This notebook is for inspection, not benchmark timing.\n"),
    code('%%configure -f\n{"conf":{"spark.executor.instances":"2","spark.executor.cores":"2","spark.executor.memory":"6g","spark.executor.memoryOverhead":"2g","spark.dynamicAllocation.enabled":"false","spark.yarn.dist.archives":"s3://inf2006-team07-bigdata-1-310947418945-us-east-1-an/dependencies/spark-numpy-env.tar.gz#environment","spark.pyspark.python":"./environment/bin/python","spark.executorEnv.PYSPARK_PYTHON":"./environment/bin/python"}}\n'),
    code('BUCKET = "inf2006-team07-bigdata-1-310947418945-us-east-1-an"\nATTEMPT = "attempt-02"  # use "" for runs directly under 18th-nov\nRUN = "run01-reference-fullprofile"  # change to the run to inspect\nROOT = f"s3://{BUCKET}/benchmark_runs/18th-nov"\nBASE = f"{ROOT}/{ATTEMPT}/{RUN}" if ATTEMPT else f"{ROOT}/{RUN}"\nprint(BASE)\n'),
    code('spark.read.option("multiLine", True).json(f"{BASE}/records/timing.json").show(truncate=False)\nspark.read.option("multiLine", True).json(f"{BASE}/spark/evidence/*/benchmark_spark_record.json").select("run_id", "database", "config", "application_id", "gdelt_summary").show(truncate=False)\n'),
    code('prices = spark.read.parquet(f"{BASE}/records/hive_tables/prices_typed")\nnews = spark.read.parquet(f"{BASE}/records/hive_tables/gdelt_daily_typed")\nprint("prices:", prices.count(), "GDELT days:", news.count())\nnews.orderBy("date_added").show(10, truncate=False)\n'),
    code('spark.read.text(f"{BASE}/records/evidence/stage2_streaming_vs_local.txt").show(100, truncate=False)\n'),
]
nb = {"cells": cells, "metadata": {"kernelspec": {"display_name": "PySpark", "language": "python", "name": "pysparkkernel"}, "language_info": {"name": "pyspark", "version": "3"}}, "nbformat": 4, "nbformat_minor": 5}
out = Path(__file__).resolve().parent.parent / "notebooks" / "benchmark_evidence_viewer.ipynb"
out.write_text(json.dumps(nb, indent=1), encoding="utf-8")
print(out)
