"""Task 4: PySpark analytics on prepared data and model outputs; only small aggregates reach pandas/matplotlib.

Q1  Unusual trading volume (vs each ticker's own previous 60 bars) and same-day absolute price moves.
Q3  Where is the model most/least reliable? Test accuracy vs the majority-class baseline by quarter and
    volatility group (cut-points taken from TRAIN vol20 terciles, so the test set does not influence them),
    plus the confusion matrix.
(Q2, news signals, needs the GDELT window to be ingested - not run here.)
"""
import argparse
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, Window, functions as F

p = argparse.ArgumentParser()
p.add_argument("--data", default="work/prepared")
p.add_argument("--tables", default="evidence/local/t4")
p.add_argument("--figs", default="report/figures")
a = p.parse_args()
for d in (a.tables, a.figs):
    os.makedirs(d, exist_ok=True)

spark = SparkSession.builder.appName("t4_analytics").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ------------------------------------------------------------------ Q1: unusual volume vs price moves
clean = spark.read.parquet(f"{a.data}/clean")
w = Window.partitionBy("ticker").orderBy("date")
prev60 = w.rowsBetween(-60, -1)
v = (clean.withColumn("n_prev", F.count("volume").over(prev60))
     .withColumn("vz", (F.col("volume") - F.avg("volume").over(prev60)) / F.stddev("volume").over(prev60))
     .withColumn("abs_ret", F.abs(F.col("close") / F.lag("close", 1).over(w) - 1))
     .where("n_prev = 60 and abs_ret is not null and vz is not null"))
bucket = (F.when(F.col("vz") < -1, "1_low (z<-1)").when(F.col("vz") <= 1, "2_normal (|z|<=1)")
          .when(F.col("vz") <= 2, "3_high (1<z<=2)").otherwise("4_very_high (z>2)"))
q1 = (v.withColumn("bucket", bucket).groupBy("bucket")
      .agg(F.count("*").alias("n"), F.avg("abs_ret").alias("mean_abs_ret"),
           F.expr("percentile_approx(abs_ret, 0.5)").alias("median_abs_ret"),
           F.expr("percentile_approx(abs_ret, 0.9)").alias("p90_abs_ret"))
      .orderBy("bucket").toPandas())
q1.to_csv(f"{a.tables}/q1_volume_vs_abs_return.csv", index=False)
print(q1.to_string(index=False))

fig, ax = plt.subplots(figsize=(7, 4))
ax.bar(q1["bucket"], q1["mean_abs_ret"] * 100, color="#4C78A8", label="mean |daily return|")
ax.plot(q1["bucket"], q1["median_abs_ret"] * 100, "o-", color="#F58518", label="median")
for i, n in enumerate(q1["n"]):
    ax.annotate(f"n={n:,}", (i, 0), ha="center", va="bottom", fontsize=8, color="white")
ax.set_ylabel("absolute same-day return (%)")
ax.set_title("Q1: unusual volume (z vs own prior 60 bars) and absolute price moves")
ax.legend()
plt.xticks(rotation=15)
plt.tight_layout()
plt.savefig(f"{a.figs}/q1_volume_vs_abs_return.png", dpi=150)
plt.close()

# ------------------------------------------------------------------ Q3: model reliability on the test set
train = spark.read.parquet(f"{a.data}/train")
c1, c2 = train.approxQuantile("vol20", [0.33, 0.67], 0.001)
pred = spark.read.parquet(f"{a.data}/test_predictions").withColumn("label", F.col("label").cast("double"))
majority = 1.0 if train.agg(F.avg(F.col("label").cast("double"))).first()[0] >= 0.5 else 0.0
pred = (pred.withColumn("vol_group", F.when(F.col("vol20") <= c1, "low_vol").when(F.col("vol20") <= c2, "mid_vol")
                        .otherwise("high_vol"))
        .withColumn("quarter", F.concat(F.year("anchor_date"), F.lit("Q"), F.quarter("anchor_date")))
        .withColumn("model_ok", (F.col("prediction") == F.col("label")).cast("int"))
        .withColumn("base_ok", (F.lit(majority) == F.col("label")).cast("int")))


def by(col):
    return (pred.groupBy(col).agg(F.count("*").alias("n"), F.avg("model_ok").alias("model_acc"),
                                  F.avg("base_ok").alias("baseline_acc"), F.avg("label").alias("share_up"),
                                  F.avg("prediction").alias("share_pred_up"))
            .withColumn("model_minus_baseline", F.col("model_acc") - F.col("baseline_acc"))
            .orderBy(col).toPandas())


by_vol, by_q = by("vol_group"), by("quarter")
by_vol.to_csv(f"{a.tables}/q3_by_volatility_group.csv", index=False)
by_q.to_csv(f"{a.tables}/q3_by_quarter.csv", index=False)
cm = (pred.groupBy("label", "prediction").count().orderBy("label", "prediction").toPandas())
cm.to_csv(f"{a.tables}/q3_confusion_matrix.csv", index=False)
print(f"vol20 tercile cut-points from TRAIN: {c1:.5f}, {c2:.5f}; majority class = {majority}")
print(by_vol.to_string(index=False))
print(by_q.to_string(index=False))
print(cm.to_string(index=False))

fig, axes = plt.subplots(1, 3, figsize=(14, 4))
for ax, df, key, title in ((axes[0], by_vol.set_index("vol_group").loc[["low_vol", "mid_vol", "high_vol"]].reset_index(),
                            "vol_group", "by volatility group"), (axes[1], by_q, "quarter", "by quarter")):
    x = range(len(df))
    ax.bar([i - 0.2 for i in x], df["model_acc"], 0.4, label="model", color="#4C78A8")
    ax.bar([i + 0.2 for i in x], df["baseline_acc"], 0.4, label="majority baseline", color="#BAB0AC")
    ax.set_xticks(list(x))
    ax.set_xticklabels([f"{k}\n(n={n:,})" for k, n in zip(df[key], df["n"])], fontsize=8)
    ax.set_ylim(0.4, 0.7)
    ax.set_title(f"Q3: test accuracy {title}")
    ax.legend(fontsize=8)
mat = cm.pivot(index="label", columns="prediction", values="count").fillna(0)
axes[2].imshow(mat.values, cmap="Blues")
for i in range(mat.shape[0]):
    for j in range(mat.shape[1]):
        axes[2].text(j, i, f"{int(mat.values[i, j]):,}", ha="center", va="center")
axes[2].set_xticks(range(mat.shape[1]))
axes[2].set_xticklabels([f"pred {int(c)}" for c in mat.columns])
axes[2].set_yticks(range(mat.shape[0]))
axes[2].set_yticklabels([f"actual {int(r)}" for r in mat.index])
axes[2].set_title("Q3: test confusion matrix")
plt.tight_layout()
plt.savefig(f"{a.figs}/q3_model_reliability.png", dpi=150)
plt.close()
