"""Task 3: train/evaluate Spark ML classifiers for next-week direction (train -> validation -> frozen test).

Experiments (feature set x model x small grid) are scored on VALIDATION only. The best configuration is
frozen, then scored once on TEST. The fitted PipelineModel (Imputer -> Assembler -> Scaler -> classifier)
is saved so t3_reload.py can reproduce its predictions in a fresh session.
"""
import argparse
import itertools
import json
import os

from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import Imputer, StandardScaler, VectorAssembler
from pyspark.ml.functions import array_to_vector, vector_to_array
from pyspark.sql import SparkSession, functions as F

p = argparse.ArgumentParser()
p.add_argument("--data", default="work/prepared")
p.add_argument("--model-out", default="models/final_pipeline")
p.add_argument("--evidence", default="evidence/local")
p.add_argument("--seed", type=int, default=42)
a = p.parse_args()

spark = SparkSession.builder.appName("t3_train").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
os.makedirs(a.evidence, exist_ok=True)

load = lambda s: spark.read.parquet(f"{a.data}/{s}").withColumn("label", F.col("label").cast("double"))
train, val, test = load("train").cache(), load("validation").cache(), load("test").cache()

FEATURE_SETS = {
    "momentum": ["mom1", "mom5", "mom20", "ma20gap"],
    "all_price": ["mom1", "mom5", "mom20", "ma20gap", "vol5", "vol20", "volratio", "range5", "gap_days"],
}
auc_ev = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
mc = lambda m: MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName=m)


def metrics(df):
    return {"auc": auc_ev.evaluate(df), "accuracy": mc("accuracy").evaluate(df), "f1": mc("f1").evaluate(df),
            "rows": df.count()}


def build(fs, kind, params):
    cols = FEATURE_SETS[fs]
    stages = [Imputer(strategy="median", inputCols=cols, outputCols=[c + "_i" for c in cols]),
              VectorAssembler(inputCols=[c + "_i" for c in cols], outputCol="raw_features"),
              StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)]
    clf = {"logreg": lambda: LogisticRegression(maxIter=100, **params),
           "rf": lambda: RandomForestClassifier(seed=a.seed, **params),
           "gbt": lambda: GBTClassifier(seed=a.seed, **params)}[kind]()
    return Pipeline(stages=stages + [clf])


GRID = {"logreg": [{"regParam": r} for r in (0.0, 0.01, 0.1)],
        "rf": [{"numTrees": n, "maxDepth": d} for n, d in ((50, 4), (100, 6))],
        "gbt": [{"maxIter": 30, "maxDepth": d} for d in (2, 3)]}

# ---------------------------------------------------------------- baselines (same observations)
majority = 1.0 if train.agg(F.avg("label")).first()[0] >= 0.5 else 0.0


def baselines(df):
    m = df.withColumn("prediction", F.lit(majority))
    r = (df.withColumn("prediction", F.when(F.col("mom5") > 0, 1.0).otherwise(0.0))
         .withColumn("rawPrediction", array_to_vector(F.array(-F.col("mom5"), F.col("mom5")))))
    # a constant prediction has AUC 0.5 by definition
    return {"majority_class": {"auc": 0.5, "accuracy": mc("accuracy").evaluate(m), "f1": mc("f1").evaluate(m)},
            "momentum_sign(mom5>0)": {"auc": auc_ev.evaluate(r), "accuracy": mc("accuracy").evaluate(r),
                                      "f1": mc("f1").evaluate(r)}}


# ---------------------------------------------------------------- experiments: validation only
results = []
for fs, kind in itertools.product(FEATURE_SETS, GRID):
    for params in GRID[kind]:
        m = build(fs, kind, params).fit(train)
        results.append({"feature_set": fs, "model": kind, "params": params,
                        "train": metrics(m.transform(train)), "validation": metrics(m.transform(val))})
        print("EXP", fs, kind, params, "val_auc=%.4f val_acc=%.4f" % (results[-1]["validation"]["auc"],
                                                                      results[-1]["validation"]["accuracy"]))
best = max(results, key=lambda r: r["validation"]["auc"])
print("BEST (validation AUC):", best["feature_set"], best["model"], best["params"])

# ---------------------------------------------------------------- freeze, then one test evaluation
final = build(best["feature_set"], best["model"], best["params"]).fit(train)
final.write().overwrite().save(a.model_out)
test_pred = final.transform(test)
test_metrics = metrics(test_pred)
(test_pred.withColumn("prob_up", vector_to_array("probability")[1])
 .select("ticker", "anchor_date", "next_anchor_date", "label", "prediction", "prob_up", "ret_next", "vol20", "volratio")
 .write.mode("overwrite").parquet(f"{a.data}/test_predictions"))

out = {"seed": a.seed, "majority_class_from_train": majority,
       "baselines": {"validation": baselines(val), "test": baselines(test)},
       "experiments": results, "selected": {k: best[k] for k in ("feature_set", "model", "params")},
       "selected_validation": best["validation"], "test_final": test_metrics, "model_path": a.model_out}
json.dump(out, open(f"{a.evidence}/t3_results.json", "w"), indent=2)
print("BASELINES test", json.dumps(out["baselines"]["test"]))
print("TEST FINAL", json.dumps(test_metrics))
