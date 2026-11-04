# EMR run evidence and submission status

Run profile: `v2026-10-29-q1-2016` (91 GDELT days, Q1 2016 development profile)
Run date: 2026-11-04
Cluster run: EMR cluster `j-06878942C5H7E0OZ24GA`
Spark application recorded by the notebook: `application_1793775642011_0010`
Spark: `3.5.5-amzn-1`; Python: `3.9.25`
Configuration: 2 executors, 2 cores/executor, 6 GiB executor memory, 2 GiB overhead, 2 GiB driver; dynamic allocation off.

## Evidence included

- `stage1_*`: cluster details, HDFS inventory/listing, DistCp log and storage report.
- `stage2_*`: Hadoop Streaming, Hive registration/transformation, result checks and local-vs-EMR audit.
- `stage3/`: PySpark preparation/reconciliation, split report, model training results and reload logs.
- `stage4/`: analytics log, CSV result tables and generated figures.
- `notebook_outputs/evidence/reference/`: environment, reconciliation, split, model metrics, analytics, reload checks and verification record.
- `notebook_outputs/evidence/perf/`: three recorded timing repetitions and resource metrics for the reference configuration.
- `notebooks/inf2006_pipeline.ipynb`: executed notebook exported from the S3-backed JupyterHub workspace, including cell outputs.
- `models/v2026-10-29-q1-2016/{direction,high_vol}/`: saved Spark ML PipelineModel directories.

## Important interpretation limits

- The recorded notebook verification has 34/34 checks passing. Both saved models were loaded and rescored without retraining; saved-vs-rescored probability difference was 0.0 and class predictions matched.
- The recorded training and reload YARN application IDs are both `application_1793775642011_0010`. Therefore this evidence demonstrates model save/load verification in the recorded Spark application, but **does not by itself prove a fresh EMR/Spark application reload**. Run the reload-only section in a newly started Spark/Livy session and retain that new application ID before claiming fresh-session verification.
- The performance JSON records three repetitions for the reference configuration on the 91-day Q1 2016 profile. It is not a completed comparison against an alternative cluster configuration or a confirmed full assessed workload.
- The 91-day GDELT profile is identified as a development profile. Do not describe it as the full assessed corpus unless the teaching team confirms that scope.
- Raw source datasets and bulky EMR daemon logs are intentionally not included in this repository package. The data acquisition/profile manifests and compact run evidence are included; raw data remains in S3.
- The earlier `evidence/emr_ref_m5xlarge/` folder is retained as historical/reference evidence and is distinct from the dated 2026-11-04 run folder.

## Before final submission

1. Run the reload-only verification from a fresh Spark/Livy application; record its distinct application ID and updated verification output.
2. Complete any required alternative-configuration/full-workload performance comparison once the assessed workload is confirmed.
3. Prepare `report.pdf` and appendices, including the exact verification record, workload limitations, AI-use declaration and team contributions.
4. Re-export the notebook after the final verification and update this package's evidence/model artifacts.
