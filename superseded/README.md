# superseded/ - kept for the record, NOT part of the assessed result

These files come from an **earlier script-based development run** (standalone `spark-submit` scripts, Oct-Nov 2026).
That run selected a different final model (random forest, test AUC 0.4990, n = 12,123 rows in its figure) than the
assessed notebook run (logistic regression, test AUC 0.5001, n = 25,533). They are kept only so that nothing
is lost; **do not cite them as evidence**. The assessed implementation is `notebooks/inf2006_pipeline.ipynb`
(built from `notebooks/cells/*.cells`) and its evidence under `evidence/`.

| Folder | What it is |
|---|---|
| `src_pipeline/`, `src_model/`, `src_analytics/` | the old standalone scripts replaced by the notebook |
| `evidence_emr_2026-11-04_script_run/` | stage 3/4 logs, tables and figures of the old scripts run on EMR |
| `evidence_local_script_run/` | laptop outputs of the old scripts (local development only) |
| `report_figures/` | figures produced by the old scripts (the report uses `report/figures/nb_*.png`) |
| `models_final_pipeline_oct22_local_run/` | model saved by the old local script (Spark 3.5.3, 22 Oct) - not the submitted model |

Remove this folder from the submission ZIP (`scripts/build_submission_zip.py` already excludes it).
