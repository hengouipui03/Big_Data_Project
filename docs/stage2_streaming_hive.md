# Stage 2 — Hadoop Streaming audit job and Hive tables

Goal: the first two required engines doing real work on the cluster, with job IDs and timings.

| Engine | Contribution in this stage | Brief |
|---|---|---|
| Hadoop Streaming | `gdelt_mapper.py` / `gdelt_reducer.py` run as a MapReduce job over the raw GDELT files in HDFS, producing the per-day audit summary (events, mean tone, date-lag count, malformed-line counters) | 2.1 "Hadoop Streaming for source processing and audit summaries" |
| Hive (on Tez) | registers the raw price file and the streaming output as external tables (no data movement), computes the table-level quality audit in SQL, writes typed Parquet tables and the per-year coverage table | 2.1 "Hive for table registration and SQL transformations"; 2.3 "Hive tables can be used by Spark" |
| HDFS | raw in `/raw/...`, audit output in `/audit/...`, Hive-managed tables in `/user/hive/warehouse/inf2006.db/` | 2.1 "keep raw, cleaned and rejected data distinct" |

Cleaning decisions (dropping rows, rejected-row store, features, splits) are deliberately left to
PySpark in Stage 3, so each engine's role stays distinct and justifiable.

## Run (on the primary node, repo folder, after Stage 1)

```
cd ~/Big_Data_Project && git pull
bash scripts/stage2_streaming_and_hive.sh v2026-10-27 evidence/emr_ref_m5xlarge
```

Approximate time: streaming 1-2 min for 7 GDELT days; Hive 2-4 min (first Tez session start is slow).

If `hive` is not found, run with Beeline instead:
`HIVE_CMD="beeline -u jdbc:hive2://localhost:10000 -n hadoop --silent=true" bash scripts/stage2_streaming_and_hive.sh ...`

## Expected results

| Check | Expected |
|---|---|
| `stage2_streaming_vs_local.txt` last line | `RESULT: EMR streaming output matches local run on every date key` |
| `20151201` row in `stage2_streaming_output.tsv` | `228333  -2.1441  8211` |
| `stage2_streaming.log` | contains `Submitted application application_...` and `Map input records=1425234` (sum of the 7 days) |
| `stage2_hive_02_transform.log` | contains `App id application_...` (Tez) and DAG progress lines |
| `raw_prices_quality` in `stage2_hive_03_results.log` | rows_total 619040, tickers 505, date_min 2013-02-08, date_max 2018-02-07, rows_up_to_2017_12_29 605910, empty_open 11, empty_high 8, empty_low 8, empty_close 0, empty_volume 0, ohlc_inconsistent 12, nonpositive 0 |
| `raw_prices_duplicates` | 0 |
| short_series | APTV, BHF, BHGE, DWDP, DXC, EVHC, FTV, HLT, UA |
| row_counts | raw_prices 619040; prices_typed 619040; gdelt_daily_typed 7 |
| `run_log.csv` | one row per sub-stage with application ids and seconds |

The Hive numbers must equal `evidence/local/quality_summary.json` — two engines, two machines,
same answers. That reconciliation is the Task 1/Task 2 quality evidence.

## Notebook cell (JupyterHub, PySpark kernel) — Spark reads the Hive tables

```python
# Stage 2 check: Spark can see the Hive metastore tables (brief 2.3)
print(spark.sparkContext.applicationId)
spark.sql("SHOW TABLES IN inf2006").show(truncate=False)
spark.sql("SELECT * FROM inf2006.raw_prices_quality").show(truncate=False)
spark.sql("SELECT yr, n_rows, tickers, first_day, last_day FROM inf2006.prices_yearly_coverage ORDER BY yr").show()
spark.sql("SELECT * FROM inf2006.gdelt_daily_typed ORDER BY date_added").show()
print(spark.table("inf2006.prices_typed").count())   # expect 619040
```

Save the notebook. Screenshot: `07_spark_reads_hive.png`.

## Known behaviour

- The Spark notebook session blocks the streaming/Hive jobs (fixed executor allocation fills YARN).
  Shut the kernel down before running this script; restart it afterwards.
- If the Hive transform step fails after the streaming job succeeded, resume with
  `FROM=c bash scripts/stage2_streaming_and_hive.sh ...` (a = streaming, b = register, c = transform, d = results).

## Save evidence

```
aws s3 cp evidence/emr_ref_m5xlarge s3://<bucket>/evidence/emr_ref_m5xlarge/ --recursive
```
then from the Mac: `scp -i ~/Downloads/labsuser.pem -r hadoop@<dns>:Big_Data_Project/evidence/emr_ref_m5xlarge evidence/`
and commit.

## Notes for the report / Appendix A

- The Hive metastore on EMR lives on the primary node and is lost with the cluster. Re-running
  `01_register_raw.hql` and `02_quality_and_typed.hql` is therefore part of every rerun (they take
  minutes). This is recorded in Appendix A section 3 ("Explain Hive table creation and access from Spark").
- External tables point at the raw HDFS paths; dropping them never touches the raw files.
- Streaming reducers: 2 (one per core node) so the reduce phase is also distributed; the output is
  two part files, which Hive reads as one table.
- `run_log.csv` is the single timing record for all stages; Stage 1 staging (44 s, excluded from the
  60-minute run) can be added as a row by hand from `stage1_cluster.txt`.
