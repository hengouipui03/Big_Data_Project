# INF2006 Big Data Project

End-to-end pipeline: raw financial data -> HDFS on AWS EMR -> Hadoop Streaming / Hive / PySpark -> Spark ML model -> analytics.

## Folder layout

| Path | What it is |
|---|---|
| `config/sources.json` | Download link and version note for every raw source (from the project brief) |
| `src/acquire/download_raw.py` | Downloads raw sources and records exact download time + checksum |
| `src/pipeline/t1_profile_raw.py` | Task 1 checker: inventory, checksums and raw-data quality summary |
| `src/streaming/gdelt_mapper.py`, `gdelt_reducer.py` | Hadoop Streaming scripts: daily GDELT audit summary |
| `evidence/local/` | Outputs produced on a laptop (labelled *local*; not the assessed EMR run) |
| `data/` | Raw data. **Never committed** (see `.gitignore`) |

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
