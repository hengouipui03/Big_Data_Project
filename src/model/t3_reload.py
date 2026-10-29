"""Task 3.3: fresh-session check - load the saved PipelineModel (no retraining) and compare predictions.

Re-scores the test set with the loaded model and compares prob_up with the predictions saved at training
time, within --tol. Exit code 1 if any check fails.
"""
import argparse
import json
import os
import sys

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession, functions as F

p = argparse.ArgumentParser()
p.add_argument("--data", default="work/prepared")
p.add_argument("--model", default="models/final_pipeline")
p.add_argument("--evidence", default="evidence/local")
p.add_argument("--tol", type=float, default=1e-9)
a = p.parse_args()

spark = SparkSession.builder.appName("t3_reload").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
res = {"model_path": a.model, "tolerance": a.tol, "checks": []}


def check(name, ok, detail=""):
    res["checks"].append({"check": name, "passed": bool(ok), "detail": detail})
    print("PASS" if ok else "FAIL", name, detail)


for part in ("metadata", "stages"):
    check(f"model dir has {part}/", os.path.isdir(os.path.join(a.model, part)))
model = PipelineModel.load(a.model)
check("PipelineModel loads", True, " -> ".join(type(s).__name__ for s in model.stages))

test = spark.read.parquet(f"{a.data}/test").withColumn("label", F.col("label").cast("double"))
saved = spark.read.parquet(f"{a.data}/test_predictions")
new = model.transform(test).withColumn("prob_new", vector_to_array("probability")[1])
j = new.join(saved.select("ticker", "anchor_date", "prob_up", F.col("prediction").alias("pred_saved")),
             ["ticker", "anchor_date"])
n_new, n_saved, n_join = new.count(), saved.count(), j.count()
check("row counts match", n_new == n_saved == n_join, f"new={n_new} saved={n_saved} joined={n_join}")
mx = j.agg(F.max(F.abs(F.col("prob_new") - F.col("prob_up")))).first()[0]
check(f"max |prob diff| <= {a.tol}", mx is not None and mx <= a.tol, f"max_abs_diff={mx}")
flips = j.where(F.col("prediction") != F.col("pred_saved")).count()
check("no class-label differences", flips == 0, f"differing_predictions={flips}")
res["max_abs_prob_diff"] = mx
json.dump(res, open(f"{a.evidence}/t3_reload.json", "w"), indent=2)
sys.exit(0 if all(c["passed"] for c in res["checks"]) else 1)
