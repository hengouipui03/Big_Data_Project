"""LOCAL DEVELOPMENT ONLY: run the notebook's code cells as one script under spark-submit (Docker).

Emulates the sparkmagic PySpark kernel: `spark` is predefined, `%%configure` is skipped, `%%local` cells run
as normal code and `%%spark -o X` cells run their body and then turn the Spark DataFrame X into pandas.
Usage:  ./run_spark.sh scripts/run_notebook_local.py notebooks/inf2006_pipeline.ipynb [VERSION] [CONFIG]
Optional env overrides: NB_VERSION, NB_CONFIG, NB_REPS. This is NOT the assessed EMR run.
"""
import json
import os
import re
import sys
import time
import traceback

from pyspark.sql import SparkSession

nb = json.load(open(sys.argv[1]))
spark = SparkSession.builder.appName("notebook_local").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
g = {"spark": spark, "__name__": "__main__"}
overrides = {"VERSION": os.environ.get("NB_VERSION"), "ACTIVE_CONFIG": os.environ.get("NB_CONFIG"),
             "REPETITIONS": os.environ.get("NB_REPS")}

n = 0
t_all = time.time()
for idx, c in enumerate(nb["cells"]):
    if c["cell_type"] != "code":
        continue
    src = "".join(c["source"])
    first = src.split("\n")[0]
    post = ""
    if first.startswith("%%configure"):
        continue
    if first.startswith("%%local"):
        src = "\n".join(src.split("\n")[1:])
    elif first.startswith("%%spark"):
        m = re.search(r"-o\s+(\w+)", first)
        src = "\n".join(src.split("\n")[1:])
        if m:
            post = f"\n{m.group(1)} = {m.group(1)}.toPandas()"
    # apply parameter overrides by rewriting the assignment lines in the parameters cell
    if re.search(r"^VERSION\s*=", src, flags=re.M):
        for k, v in overrides.items():
            if v:
                val = v if k == "REPETITIONS" else repr(v)
                src = re.sub(rf"^{k}\s*=\s*[^\n#]*", lambda m: f"{k} = {val}  ", src, count=1, flags=re.M)
    n += 1
    t0 = time.time()
    try:
        exec(compile(src + post, f"<cell {idx}>", "exec"), g)
    except Exception:
        print(f"\n######## CELL {idx} FAILED ########\n{src[:400]}\n---")
        traceback.print_exc()
        sys.exit(1)
    print(f"-- cell {idx} done in {time.time() - t0:.1f}s")
print(f"\nALL {n} CODE CELLS RAN in {time.time() - t_all:.0f}s")
