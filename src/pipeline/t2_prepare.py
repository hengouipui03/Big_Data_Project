"""Task 2: clean prices, build weekly targets/features, create chronological splits (PySpark).

Zones written under --out:  clean/  rejected/  weekly_all/  train/  validation/  test/
Evidence written under --evidence:  t2_reconciliation.json, t2_split_report.json

Definitions
- Week block: Friday..Thursday. anchor = first trading day of the block (normally the Friday).
- Target: ret_next = open(next anchor) / open(anchor) - 1; label = 1 if ret_next > 0 else 0.
- Features use only bars strictly before the anchor day (as of the previous trading bar).
- A row is kept in a split only if its target outcome is known inside that split's period.
"""
import argparse
import json
import os

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import StringType, StructField, StructType

p = argparse.ArgumentParser()
p.add_argument("--prices", default="data/raw/prices/all_stocks_5yrs.csv")
p.add_argument("--out", default="work/prepared")
p.add_argument("--evidence", default="evidence/local")
p.add_argument("--data-end", default="2017-12-29")
p.add_argument("--train-end", default="2015-12-31")
p.add_argument("--val-start", default="2016-01-01")
p.add_argument("--val-end", default="2016-12-31")
p.add_argument("--test-start", default="2017-01-01")
p.add_argument("--min-history", type=int, default=61)
a = p.parse_args()

spark = SparkSession.builder.appName("t2_prepare").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
os.makedirs(a.evidence, exist_ok=True)

# ---------------------------------------------------------------- 1. read raw, clean, reject
cols = ["date", "open", "high", "low", "close", "volume", "Name"]
raw = (spark.read.option("header", True).option("mode", "PERMISSIVE")
       .schema(StructType([StructField(c, StringType()) for c in cols]))
       .csv(a.prices).withColumn("_src_file", F.input_file_name()))
n_raw = raw.count()

num = ["open", "high", "low", "close", "volume"]
parsed = raw.select(F.to_date("date").alias("date"), F.col("Name").alias("ticker"),
                    *[F.col(c).cast("double").alias(c) for c in num], "_src_file")

prices = [F.col(c) for c in ("open", "high", "low", "close")]
reason = (F.when(F.col("date").isNull() | F.col("ticker").isNull() | (F.col("ticker") == ""), "bad_key")
          .when(F.col("date") > F.lit(a.data_end).cast("date"), "out_of_scope_date")
          .when(F.col("close").isNull() | F.col("volume").isNull(), "missing_close_or_volume")
          .when(F.col("open").isNull() | F.col("high").isNull() | F.col("low").isNull(), "missing_ohlc_field")
          .when(F.greatest(*[F.when(c <= 0, 1).otherwise(0) for c in prices]) == 1, "nonpositive_price")
          .when(F.col("volume") < 0, "negative_volume")
          .when((F.col("high") < F.col("low")) | (F.col("high") < F.col("open")) | (F.col("high") < F.col("close"))
                | (F.col("low") > F.col("open")) | (F.col("low") > F.col("close")), "ohlc_inconsistent"))
tagged = parsed.withColumn("reject_reason", reason)

ok = tagged.where(F.col("reject_reason").isNull())
dedup_w = Window.partitionBy("ticker", "date").orderBy("_src_file")
ok = ok.withColumn("_dup", F.row_number().over(dedup_w))
dups = ok.where(F.col("_dup") > 1).withColumn("reject_reason", F.lit("duplicate_key")).drop("_dup")
clean = ok.where(F.col("_dup") == 1).drop("_dup", "reject_reason")
rejected = tagged.where(F.col("reject_reason").isNotNull()).unionByName(dups)

clean.write.mode("overwrite").parquet(f"{a.out}/clean")
rejected.write.mode("overwrite").parquet(f"{a.out}/rejected")
clean = spark.read.parquet(f"{a.out}/clean")
rejected = spark.read.parquet(f"{a.out}/rejected")

n_clean, n_rej = clean.count(), rejected.count()
rej_by = {r["reject_reason"]: r["n"] for r in rejected.groupBy("reject_reason").agg(F.count("*").alias("n")).collect()}
recon = {"raw_rows": n_raw, "clean_rows": n_clean, "rejected_rows": n_rej, "rejected_by_reason": rej_by,
         "reconciles": n_raw == n_clean + n_rej,
         "clean_rows_up_to_data_end": n_clean}
json.dump(recon, open(f"{a.evidence}/t2_reconciliation.json", "w"), indent=2)
print("RECON", json.dumps(recon))
assert recon["reconciles"], "raw != clean + rejected"

# ---------------------------------------------------------------- 2. daily as-of features
w = Window.partitionBy("ticker").orderBy("date")
w5, w20, w60 = w.rowsBetween(-4, 0), w.rowsBetween(-19, 0), w.rowsBetween(-59, 0)
d = (clean.withColumn("rn", F.row_number().over(w))
     .withColumn("mom1", F.col("close") / F.lag("close", 1).over(w) - 1)
     .withColumn("mom5", F.col("close") / F.lag("close", 5).over(w) - 1)
     .withColumn("mom20", F.col("close") / F.lag("close", 20).over(w) - 1)
     .withColumn("vol5", F.stddev("mom1").over(w5))
     .withColumn("vol20", F.stddev("mom1").over(w20))
     .withColumn("volratio", F.avg("volume").over(w5) / F.avg("volume").over(w60))
     .withColumn("range5", F.avg((F.col("high") - F.col("low")) / F.col("close")).over(w5))
     .withColumn("ma20gap", F.col("close") / F.avg("close").over(w20) - 1))
feats = ["mom1", "mom5", "mom20", "vol5", "vol20", "volratio", "range5", "ma20gap"]

# shift by one bar so the features seen at day t describe information up to the previous bar only
for c in feats:
    d = d.withColumn(c, F.lag(c, 1).over(w))
d = (d.withColumn("hist_n", F.lag("rn", 1).over(w))
     .withColumn("gap_days", F.datediff("date", F.lag("date", 1).over(w))))
feats.append("gap_days")

# ---------------------------------------------------------------- 3. weekly anchors and targets
d = d.withColumn("block", F.date_sub("date", (F.dayofweek("date") + 1) % 7))
bw = Window.partitionBy("ticker", "block").orderBy("date")
anchors = d.withColumn("_k", F.row_number().over(bw)).where("_k = 1").drop("_k")
aw = Window.partitionBy("ticker").orderBy("block")
anchors = (anchors.withColumn("next_block", F.lead("block").over(aw))
           .withColumn("next_anchor_date", F.lead("date").over(aw))
           .withColumn("next_open", F.lead("open").over(aw))
           .withColumn("anchor_open", F.col("open"))
           .withColumn("ret_next", F.col("next_open") / F.col("open") - 1)
           .withColumn("label", F.when(F.col("ret_next").isNull(), None)
                       .otherwise((F.col("ret_next") > 0).cast("int"))))

split = (F.when(F.col("date") <= F.lit(a.train_end).cast("date"), "train")
         .when((F.col("date") >= F.lit(a.val_start).cast("date")) & (F.col("date") <= F.lit(a.val_end).cast("date")), "validation")
         .when(F.col("date") >= F.lit(a.test_start).cast("date"), "test"))
split_end = (F.when(F.col("split") == "train", F.lit(a.train_end).cast("date"))
             .when(F.col("split") == "validation", F.lit(a.val_end).cast("date"))
             .otherwise(F.lit(a.data_end).cast("date")))
status = (F.when(F.col("hist_n").isNull() | (F.col("hist_n") < a.min_history), "insufficient_history")
          .when(F.col("next_block").isNull(), "no_next_anchor")
          .when(F.col("next_block") != F.date_add("block", 7), "gap_next_block")
          .when(F.col("open").isNull() | F.col("next_open").isNull(), "missing_open")
          .when(F.col("next_anchor_date") > split_end, "target_crosses_boundary")
          .otherwise("ok"))
weekly = (anchors.withColumn("split", split).withColumn("status", status)
          .withColumnRenamed("date", "anchor_date")
          .select("ticker", "anchor_date", "next_anchor_date", "split", "status", "anchor_open", "next_open",
                  "ret_next", "label", *feats))
weekly.write.mode("overwrite").parquet(f"{a.out}/weekly_all")
weekly = spark.read.parquet(f"{a.out}/weekly_all")

for s in ("train", "validation", "test"):
    (weekly.where((F.col("split") == s) & (F.col("status") == "ok"))
     .write.mode("overwrite").parquet(f"{a.out}/{s}"))

# ---------------------------------------------------------------- 4. split report
report = {}
for s in ("train", "validation", "test"):
    sub = weekly.where(F.col("split") == s)
    okd = sub.where("status = 'ok'")
    agg = okd.agg(F.count("*").alias("rows"), F.min("anchor_date").cast("string").alias("first_anchor"),
                  F.max("anchor_date").cast("string").alias("last_anchor"),
                  F.max("next_anchor_date").cast("string").alias("last_outcome_date"),
                  F.countDistinct("ticker").alias("tickers"), F.avg("label").alias("share_up")).first().asDict()
    agg["anchor_rows_total"] = sub.count()
    agg["excluded_by_reason"] = {r["status"]: r["n"] for r in
                                 sub.where("status != 'ok'").groupBy("status").agg(F.count("*").alias("n")).collect()}
    report[s] = agg
json.dump(report, open(f"{a.evidence}/t2_split_report.json", "w"), indent=2, default=str)
print("SPLITS", json.dumps(report, indent=2, default=str))
