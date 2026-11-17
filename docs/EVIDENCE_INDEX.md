# Evidence index and limitations

All cell numbers below are zero based in [the submitted executed notebook](../notebooks/inf2006_pipeline.ipynb). [The text transcription](../evidence/final_run/notebook_text_outputs.txt) preserves console output for convenient review; it is a local extraction, not another cloud execution. [run_summary.json](../evidence/final_run/run_summary.json) is likewise explicitly labelled as a local transcription.

| Claim | Expected and recorded result | Evidence |
|---|---|---|
| EMR environment | Spark 3.5.5-amzn-1, Python 3.9.25; two executors, two cores and 6g heap each | notebook cell 7 |
| Staging provenance | Cluster j-0713228OYIMPDIUL3VF; EMR 7.10.0; replication 2; staging 97 seconds, excluded from processing | [stage1_cluster.txt](../evidence/final_run/stage1_cluster.txt), [HDFS inventory](../evidence/final_run/stage1_hdfs_inventory.csv) |
| Streaming | 91 daily aggregates; 18,369,793 events; local/cloud comparison succeeds | [job log](../evidence/final_run/stage2_streaming.log), [output](../evidence/final_run/stage2_streaming_output.tsv), [comparison](../evidence/final_run/stage2_streaming_vs_local.txt) |
| Actual Hive execution | Registration, Tez CTAS and SQL result evidence | [registration](../evidence/final_run/stage2_hive_01_register.log), [transforms](../evidence/final_run/stage2_hive_02_transform.log), [results](../evidence/final_run/stage2_hive_03_results.log), [DDL backup](../evidence/final_run/hive_backup/all_table_definitions.sql) |
| Price reconciliation | 619,040 raw = 605,887 clean + 13,153 rejected | notebook cells 9 and 11 |
| Chronological splits and exclusions | Train 2013-2015, validate 2016, test 2017; no crossing outcomes; training-only preprocessing | notebook cells 11 and 13 |
| Model selection | 14 validation experiments per task; frozen choices before test | notebook cell 17 |
| Final test results and baselines | direction AUC about 0.5001; high_vol AUC about 0.7261; full baseline table retained | notebook cells 17 and 21 |
| Three analyses | Q1 volume/price movement; Q2 news/subsequent behaviour over 13 weeks; Q3 subgroup reliability and confusion matrices | notebook cells 23, 25, 27; figures in [report/figures/](../report/figures/) |
| Performance | Shell stages 202/12/41/26 seconds; notebook stage sum 276.1 seconds; notebook benchmark median 45.14 seconds | [run_log.csv](../evidence/final_run/run_log.csv), notebook cells 38, 40, 42 and 44 |
| Fresh-session reload | Training ..._0006, reload ..._0007; probability difference 0.0 within 1e-9; zero changed classes, both HDFS and S3 | notebook cell 46 |
| Verification record | 34 of 34 recorded checks passed | notebook cell 48 |

The seven-day [development run log](../evidence/development/run_log.csv) and its [streaming comparison](../evidence/development/stage2_streaming_vs_local.txt) are historical EMR development evidence. [Local inventories](../evidence/local/) are labelled laptop evidence and do not replace assessed EMR execution.

## Limits that the report must retain

- The final handover covers **91 GDELT days**, not the full 731-day reference corpus. The latter was a reference/planning scope; no 731-day execution is claimed here.
- The final notebook's raw-byte and complete timing fields are null because its output version differed from the staged raw/evidence paths. Per-file raw bytes/checksums and separate shell timing are retained. Do not turn an arithmetic sum of selected stages into a measured complete-pipeline duration.
- The three benchmark repetitions (46.62, 45.14 and 41.56 seconds) cover notebook preparation and final fits, not three repetitions of the entire Streaming/Hive/analytics path. There is no complete paired configuration comparison for this final run in the handover. State this limitation rather than reuse unattributed timing.
- Reload output reports success and a fresh application. Its saved source has an older fallback VERSION after a restart. Configure VERSION, BUCKET and ACTIVE_CONFIG explicitly before running that cell, as described in the guide. The original notebook was not edited to hide this mismatch.
- Notebook verification output still names `evidence/emr_ref_m5xlarge` and some original cloud JSON filenames. These original labels are retained verbatim. The Hive locators now correspond to the final_run files linked above; absent original cloud JSONs are not claimed to be included. Use the notebook's recorded outputs for those results.
- The 34/34 record verifies its listed data/leakage/analysis conditions; it does not prove that every requirement in the brief has been met or that complete timing data is available.
- Direction does not clearly beat its majority baseline; Q2 has only 13 weekly observations and cannot establish causation. Price ratios are source-data diagnostics, not investable economic returns.
- The new standalone reload and packaging helpers were added locally. Their source validation is separate from the historical EMR output. No new cloud execution was performed during local preparation.
- The final report is absent until the team supplies `report.pdf`. Do not upload a package labelled REVIEW ONLY.
