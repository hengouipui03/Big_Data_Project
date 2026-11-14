# INF2006 Big Data Project

End-to-end pipeline: raw financial data -> HDFS on AWS EMR -> Hadoop Streaming / Hive / PySpark -> Spark ML model -> analytics.

## Folder layout

| Path | What it is |
|---|---|
| `report.pdf` | the report (cover, sections 1-7, references, Appendices A-D) - added at submission |
| `notebooks/inf2006_pipeline.ipynb` | **The PySpark notebook** (Tasks 2-4, model reload, timings), exported from EMR. Source of truth: `notebooks/cells/*.cells`, built by `scripts/build_notebook.py` (a rebuild erases outputs - re-run on EMR and export again) |
| `models/v2026-10-29-q1-2016/{direction,high_vol}/` | the saved Spark ML `PipelineModel`s (complete, including `stages/*/data/` parameter files) |
| `docs/RUN_ON_EMR.md` | **Appendix A source** - numbered EMR setup, run, reload and cleanup guide |
| `docs/data_dictionary.md` | corpus scope, schemas, units, time semantics (Task 1.2) |
| `docs/input_profiles.md` | frozen input profiles, measured rates and the 731-day projection |
| `config/emr/` | cluster configuration JSON, `EMR_cluster_history.md`, exported cluster metadata (`metadata/`) |
| `scripts/emr_run_all.sh` | on the primary node: Stage 1 (S3 -> HDFS) + Stage 2 (Hadoop Streaming, Hive). Run BEFORE the notebook |
| `scripts/build_numpy_env.sh` | builds/uploads the numpy environment the notebook needs on the executors (once per bucket) |
| `scripts/build_submission_zip.py` | pre-submission checks and ZIP builder (`--check` to only check) |
| `src/streaming/`, `sql/hive/` | Hadoop Streaming mapper/reducer; Hive HQL |
| `src/acquire/`, `src/pipeline/t1_profile_raw.py` | download with checksums; Task 1 inventory and quality profile |
| `evidence/emr_run_2026-11-04_q1-2016/` | shell-stage logs and `notebook_outputs/` of the 4 Nov run (reload in the training session) |
| `evidence/fresh_EMR_run_2026-11-14/` | notebook evidence of the 13/14 Nov run with a separate reload session |
| `evidence/gdelt_manifest/`, `evidence/local/` | acquisition manifests; laptop inventory/quality profile (local development, not the assessed run) |
| `evidence/old_emr_evidence_run_02/` | 28 Oct development-profile run (historical) |
| `report/figures/nb_*.png` | figures extracted from the executed notebook |
| `superseded/` | earlier script-based run kept for the record - **not evidence**, see its README |
| `scripts/run_notebook_local.py` | local-only test harness (Docker Spark); never counts as the assessed run |
| `data/`, `work/` | raw data and scratch. **Never committed or shipped** |

## Task 1 on a laptop (local evidence)

Requires Python 3.8+ only (no extra packages). Run every command from the repo root.

```bash
# 1. download the compulsory price file (~30 MB)
python -I src/acquire/download_raw.py prices

# 2. download a range of GDELT days (each ~15-40 MB zipped, ~100-200 MB unzipped)
python -I src/acquire/download_raw.py gdelt 20151201 20151207

# 3. build the inventory and quality summary
python -I src/pipeline/t1_profile_raw.py data/raw evidence/local config/sources.json

# 4. (optional) test the streaming scripts the way Hadoop runs them
cat data/raw/gdelt/*.export.CSV | python -I src/streaming/gdelt_mapper.py | sort -k1,1 | python -I src/streaming/gdelt_reducer.py > evidence/local/streaming_gdelt_audit.tsv
```

On Windows PowerShell, replace step 4 with:

```powershell
Get-Content data/raw/gdelt/*.export.CSV | python -I src/streaming/gdelt_mapper.py | sort | python -I src/streaming/gdelt_reducer.py > evidence/local/streaming_gdelt_audit.tsv
```

Outputs to commit: `evidence/local/acquisition_log.csv`, `inventory.csv`, `quality_summary.json`, `streaming_gdelt_audit.tsv`.

Expected checksum of the official price file (sha256): `6aea253cd19de60b568143991aaf1fa482456565c389205658d236e595e716cf`
(619,040 rows, 29,580,549 bytes). A different value means the download differs from the official file.
