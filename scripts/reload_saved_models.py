"""Load the submitted models in a fresh Spark application; never fit a model.

Requires EMR Spark and the numpy worker environment described in RUN_ON_EMR.md.
This helper was added during local packaging and is separate from the historical
successful reload output retained in the executed notebook.
"""
import argparse
import json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", default="v2026-15th-nov-2016")
    parser.add_argument("--model-root")
    parser.add_argument("--prepared-root")
    parser.add_argument("--training-app", default="application_1794811368122_0006")
    parser.add_argument("--output", default="reload_verification.json")
    args = parser.parse_args()
    from pyspark.sql import SparkSession, functions as F
    from pyspark.ml import PipelineModel
    from pyspark.ml.functions import vector_to_array
    from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
    spark = SparkSession.builder.appName("INF2006 submitted model reload").getOrCreate()
    model_root = args.model_root or f"hdfs:///models/{args.version}"
    prepared = args.prepared_root or f"hdfs:///prepared/{args.version}/reference"
    record = {"training_application_id": args.training_app, "reload_application_id": spark.sparkContext.applicationId,
              "version": args.version, "probability_tolerance": 1e-9, "metric_tolerance": 1e-6, "results": []}
    record["fresh_session"] = record["training_application_id"] != record["reload_application_id"]
    try:
        for task, expected in (("direction", 25533), ("high_vol", 25029)):
            model = PipelineModel.load(f"{model_root}/{task}")
            test = spark.read.parquet(f"{prepared}/{task}/test")
            saved = spark.read.parquet(f"{prepared}/pred_{task}").cache()
            new = model.transform(test).withColumn("prob_new", vector_to_array("probability")[1]).cache()
            joined = new.select("ticker", "anchor_date", "prob_new", F.col("prediction").alias("prediction_new")).join(
                saved.select("ticker", "anchor_date", "prob", "prediction"), ["ticker", "anchor_date"])
            counts = [new.count(), saved.count(), joined.count()]
            unique = saved.select("ticker", "anchor_date").distinct().count() == expected and new.select("ticker", "anchor_date").distinct().count() == expected
            maxdiff = joined.agg(F.max(F.abs(F.col("prob_new") - F.col("prob")))).first()[0]
            flips = joined.where(F.col("prediction_new") != F.col("prediction")).count()
            auc = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="prob", metricName="areaUnderROC")
            accuracy = MulticlassClassificationEvaluator(metricName="accuracy")
            new_for_metrics = new.drop("prob").withColumnRenamed("prob_new", "prob")
            metric_differences = {"auc": abs(auc.evaluate(new_for_metrics) - auc.evaluate(saved)),
                                  "accuracy": abs(accuracy.evaluate(new_for_metrics) - accuracy.evaluate(saved))}
            ok = counts == [expected] * 3 and unique and maxdiff is not None and maxdiff <= 1e-9 and flips == 0 and all(v <= 1e-6 for v in metric_differences.values())
            record["results"].append({"task": task, "counts": counts, "unique_keys": unique, "max_probability_difference": maxdiff,
                                      "class_changes": flips, "metric_differences": metric_differences, "passed": ok})
            new.unpersist()
            saved.unpersist()
        record["all_checks_passed"] = record["fresh_session"] and all(r["passed"] for r in record["results"])
        with open(args.output, "w", encoding="utf8") as f:
            json.dump(record, f, indent=2)
        print(json.dumps(record, indent=2))
        if not record["all_checks_passed"]:
            raise SystemExit("Reload verification failed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
