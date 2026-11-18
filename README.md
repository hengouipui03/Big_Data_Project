# INF2006 Big Data Project — Team 07

This project processes daily financial prices and GDELT events on AWS EMR using Hadoop Streaming, Hive and PySpark. It evaluates weekly price-direction and high-volatility classifiers and presents three data analyses.

| Item | Location |
| --- | --- |
| Project report | [report.pdf](report.pdf) |
| Executed notebook | [notebooks/inf2006_pipeline.ipynb](notebooks/inf2006_pipeline.ipynb) |
| EMR setup and rerun | [docs/RUN_ON_EMR.md](docs/RUN_ON_EMR.md) |
| Evidence index | [docs/EVIDENCE_INDEX.md](docs/EVIDENCE_INDEX.md) |
| Data dictionary | [docs/data_dictionary.md](docs/data_dictionary.md) |
| Input profiles and performance | [docs/input_profiles.md](docs/input_profiles.md) |
| Saved models | [models/v2026-15th-nov-2016/](models/v2026-15th-nov-2016/) |
| Prepared test data and predictions | [prepared/v2026-15th-nov-2016/reference/](prepared/v2026-15th-nov-2016/reference/) |
| Executed-run evidence | [evidence/final_run/](evidence/final_run/) |
| Input manifests | [evidence/gdelt_manifest/](evidence/gdelt_manifest/) |

The executed notebook records the training and fresh-session reload applications, test results and 34 verification checks. The report and evidence index describe the scope and coverage of the recorded timings. Raw datasets are excluded; the setup guide explains acquisition and checksum verification.
