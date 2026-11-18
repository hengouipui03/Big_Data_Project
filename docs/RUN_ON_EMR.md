# Appendix A source - setup and rerun guide

The actual executed notebook is [inf2006_pipeline.ipynb](../notebooks/inf2006_pipeline.ipynb); its byte-identical export and historical outputs are preserved.  A fresh reproduction generates new evidence; it does not change the original run.

## 1. Prerequisites and configuration

Use EMR 7.10.0, one primary and two core m5.xlarge nodes, no task nodes or automatic scaling. Enable Hadoop 3.4.1, Hive 3.1.3 on Tez, Spark 3.5.5-amzn-1, Livy and JupyterHub. The notebook recorded Python 3.9.25. Use HDFS replication 2, two executors x two cores x 6g heap, 2g executor overhead, 2g driver, no dynamic allocation, 200 shuffle partitions and AQE disabled for the reference configuration.

Configure root/data volumes and the bucket as described in [config/emr/README.md](../config/emr/README.md) and use the [reference JSON](../config/emr/cluster_configurations_reference_m5xlarge.json). The EMR service and instance roles need access to the selected bucket and normal EMR/HDFS/YARN operations; no credentials are shipped. Choose a reachable subnet and your existing key pair. Restrict SSH 22 and JupyterHub 9443 to your IP.

Choose `BUCKET` in the cluster's region and a **new, unused** notebook output `VERSION` for a full rerun. The historical version is `v2026-15th-nov-2016`. Reusing it may hit the notebook's training-session overwrite guard. The submitted cloud bucket is recorded for provenance; reproduction should use resources you control.

## 2. Environment and execution order

Connect as `hadoop` over SSH, upload/extract the submission ZIP and run commands from its root. Keep notebook and shell scripts in the same project tree. Install numpy on both driver/workers through the existing environment helper:

```bash
bash scripts/build_numpy_env.sh "$BUCKET" # END
```

Edit the notebook's first `%%configure -f` cell to point `spark.yarn.dist.archives` at your bucket's `dependencies/spark-numpy-env.tar.gz#environment`. Keep its Python worker settings. The helper uses EMR's Python environment; a local installation is not a substitute for worker dependencies.

Execution order is dependency helper, raw acquisition/upload, `emr_run_all.sh` (HDFS staging, Streaming and Hive), then the notebook. JupyterHub uses the PySpark kernel at `https://PRIMARY-DNS:9443`; use the credentials configured for your environment.

## 3. Acquire the exact inputs and stage HDFS

Python 3.10+ is sufficient for local packaging; the acquisition scripts also support the project's older Python baseline. Use the supplied source URLs in [config/sources.json](../config/sources.json):

```bash
python -I src/acquire/download_raw.py prices # END
bash scripts/acquire_gdelt.sh 20160101 20160331 data/raw/gdelt_q1_2016 # END
```

Expected price snapshot: 619,040 rows, 29,580,549 bytes, SHA-256 `6aea253cd19de60b568143991aaf1fa482456565c389205658d236e595e716cf`. Expected GDELT: 91 unzipped daily files, 18,369,793 events, 6,944,840,235 expanded bytes. Verify against the [Q1 inventory](../evidence/local/q1_2016/inventory.csv) and [consolidated manifest](../evidence/gdelt_manifest/q1_2016/manifest.tsv). The compressed size is 1,217,804,334 bytes. Do not upload ZIPs or unrelated files into the raw source prefixes.

Upload the price CSV to `s3://BUCKET/raw/prices/VERSION/all_stocks_5yr.csv` and the 91 extracted `.export.CSV` files to `s3://BUCKET/raw/gdelt/VERSION/`. Use the same chosen version for both sources and the notebook. Stage and execute on the EMR primary:

```bash
bash scripts/emr_run_all.sh "$BUCKET" "$VERSION" evidence/reproduction # END
```

The helper invokes the supplied mapper/reducer and HQL files, records HDFS SHA-256/rows/bytes and compares daily Streaming output against the Q1 local audit. Its Q1 profile selection/default version and comparison failure exit status were corrected during local packaging; those edits are not a new EMR execution. A seven-day profile needs its matching local audit instead.

Expected: 92 source files, local/HDFS checksums agree, Streaming emits 91 daily groups and the comparison succeeds. Hive creates `inf2006.raw_prices`, `gdelt_daily_audit`, typed Parquet inputs and quality audit tables. Expected price quality: 619,040 rows, 505 tickers, 605,910 rows through 2017-12-29, 11 empty opens, 8 empty highs, 8 empty lows, 12 inconsistent OHLC rows, zero duplicate ticker/date pairs. Keep this new evidence separately from `evidence/final_run`.

## 4. Execute the notebook

Upload/open the notebook, select PySpark, set VERSION to your staged version, BUCKET to your bucket and ACTIVE_CONFIG to `reference` in cell 3. Confirm both bucket locations (configuration and parameter cell). Restart and run all cells. The sequence is environment/audit, price cleaning and features, chronological splits, validation-only model experiments, frozen final fits, baseline comparison, three analyses/figures, benchmark repetitions, reload and verification.

Expected raw/clean/rejected counts: 619,040 / 605,887 / 13,153. Direction train/validation/test: 66,424 / 25,805 / 25,533; high_vol: 66,412 / 25,307 / 25,029. Train outcomes must be known by 2015-12-31, validation outcomes by 2016-12-31; test ends 2017-12-29. Random seed 42. High-volatility cutoff is learned only on training data, about 0.013936. The saved PipelineModel includes imputer, assembler, scaler and logistic regression.

Historical final choices were logistic regression/all_price with regParam 0 for direction and 0.01 for high_vol. Test AUC was approximately 0.5001 and 0.7261 respectively. All-price feature columns differ by task; the checkpoint stores the actual ordered inputs. See the data dictionary for targets, units and the next-block volatility horizon used in this project.

Historical component timing: staging 97 s (excluded); shell processing 281 s; reported main-notebook stage sum 276.1 s. The three prepare/final-fit benchmark repetitions had median 45.14 s. These are component records, not a complete median-of-three end-to-end measurement. Consult [EVIDENCE_INDEX.md](EVIDENCE_INDEX.md) before interpreting performance. A fresh run must record its own timings and memory.

## 5. Fresh-session model reload without fitting

For a full notebook rerun, restart the kernel after training; configure dependencies first, then **explicitly set VERSION, BUCKET and ACTIVE_CONFIG** to the run being reloaded before running only the reload code (zero-based cell 46). Its older default VERSION must not be used after restart. Do not rerun preparation or model-fitting cells during the reload. Preserve the new training-session record and ensure the new application ID differs.

To verify the submitted historical checkpoints without retraining, copy the shipped model and derived test/prediction folders to HDFS:

```bash
hdfs dfs -mkdir -p /models /prepared # END
hdfs dfs -put models/v2026-15th-nov-2016 /models/ # END
hdfs dfs -put prepared/v2026-15th-nov-2016 /prepared/ # END
```

These commands intentionally do not overwrite existing destinations; choose a separate root and pass helper options if those paths already exist. Start a new Spark application with the same worker environment:

```bash
PYSPARK_DRIVER_PYTHON=/tmp/spark-numpy-env/bin/python spark-submit --master yarn --deploy-mode client --archives "s3://$BUCKET/dependencies/spark-numpy-env.tar.gz#environment" --conf spark.pyspark.python=./environment/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executor.instances=2 --conf spark.executor.cores=2 --conf spark.executor.memory=6g --conf spark.executor.memoryOverhead=2g --conf spark.dynamicAllocation.enabled=false scripts/reload_saved_models.py # END
```

The standalone helper was added locally. It calls `PipelineModel.load` and `transform`, never `fit`; checks 25,533/25,029 rows, unique ticker/anchor keys, maximum probability difference <=1e-9, zero class changes and AUC/accuracy differences <=1e-6 against the shipped predictions. Driver Python must have numpy available; the environment helper and EMR configuration must supply it. It writes a new `reload_verification.json`; it has not been represented as a completed cloud run.

Dated historical proof is retained in notebook cell 46: training `application_1794811368122_0006`, reload `application_1794811368122_0007`, fresh session True and all checks passed for both HDFS and S3 checkpoint copies. No standalone original final-run JSON was fabricated to replace the exported output.

## 6. Troubleshooting and cleanup

- Missing numpy or failed Spark startup: check the archive exists, the bucket/role permissions, the Python worker settings and driver dependencies.
- Missing Hive tables: execute Streaming/Hive on the current cluster before the notebook; Spark SQL alone is insufficient.
- Training-session record already exists: choose a fresh output version for a complete rerun. Do not overwrite the historical identity record to hide a mismatch.
- Reload paths use an old version: explicitly set the three reload parameters; historical output directories are not automatically equivalent to raw prefixes.
- Missing performance sizes or shell timing: stage raw and publish the run log under the same VERSION used by the notebook. Keep any unresolved fields null and explain them.
- Compare failed Streaming/local output: stop and inspect date keys, counts, lag totals and tone tolerance before training.

Download the executed notebook, complete models and all evidence before termination. Preserve source manifests, new IDs/configuration, verification records and output locations. Terminate the EMR cluster and confirm termination; remove temporary chargeable resources according to your account procedures. The team does not need to keep a cluster running for submission. Never include keys or access tokens in the package.
