# Input profiles (brief v1.1, Task 1.1: "Freeze and document your selected development and full input profiles before comparing configurations")

Each profile is an exact list of raw files identified by sha256, so any run can be matched to its inputs
(brief: "identical input manifests").

## Development profile — FROZEN 2026-10-28

| Source | Files | Expanded bytes | Rows | Provenance |
|---|---|---|---|---|
| Daily price CSV | `all_stocks_5yr.csv` | 29,580,549 | 619,040 (+1 header) | `evidence/local/acquisition_log.csv`, sha256 `6aea253c…716cf` |
| GDELT 1.0 daily export | `20151201`–`20151207.export.CSV` (7 days) | 536,722,649 (zips: ~140 MB) | 1,425,234 events | `evidence/local/acquisition_log.csv` (zip sha256), `evidence/local/inventory.csv` (expanded sha256) |

HDFS layout: `/raw/prices/v2026-10-27/`, `/raw/gdelt/v2026-10-27/`. S3 mirror: `s3://<bucket>/raw/...`.
HDFS verification of every file: `evidence/old_emr_evidence_run_02/stage1_hdfs_inventory.csv` (all 8 sha256 equal the laptop values).

Why these 7 days: a contiguous week inside the GDELT window (Jul 2015–Jun 2017) that overlaps the price
data, small enough (0.5 GB) to test every pipeline stage in minutes on the reference cluster.

## Full profile — NOT YET FROZEN

Brief v1.1: "Any common assessed workload will be confirmed by the teaching team before full runs"
and "develop on a small sample until then". The full profile will be frozen here once the teaching team
confirms the assessed workload. Candidate: prices (same file) + a contiguous GDELT range of N days.

Sizing evidence (measured on the reference cluster; costs in seconds are wall clock):
- Hadoop Streaming audit, development profile: 42 s for 0.51 GB expanded (≈ 80 s/GB, dominated by job start-up) - `evidence/old_emr_evidence_run_02/run_log.csv`.
- Hadoop Streaming audit, Q1-2016 profile: **212 s for 6.94 GB expanded (≈ 30.5 s/GB, ≈ 33 MB/s)** - `evidence/emr_run_2026-11-04_q1-2016/run_log.csv`. This is the rate to use for projections; the 80 s/GB figure is start-up dominated and must not be extrapolated.
- HDFS capacity on the assessed clusters (2 x 100 GiB gp3 per core node): **387.75 GB configured = about 194 GB of logical data at replication 2** - `evidence/emr_run_2026-11-04_q1-2016/stage1_dfs_report.txt` (26.4 GB used at that moment = the 6.9 GB profile staged twice - versioned plus the unused flat copies, see `stage1_hdfs_inventory_flat_copies_unused.csv` - at replication 2). The earlier 115.75 GB figure belonged to the failed 28 Oct default-storage attempt only.
- GDELT expanded size ≈ 75 MB/day average (54.71 GB / 731 days; Q1-2016 averages 76 MB/day); zip ≈ 13 MB/day.
- Budget rule (brief): raw + prepared files + simultaneous copies + shuffle + temp + logs must fit the logical ceiling; compressed and expanded sizes are not interchangeable.

| Range | Days | Expanded GB | x2 replication | Streaming time (30.5 s/GB, linear projection) | Fits 194 GB logical ceiling? |
|---|---|---|---|---|---|
| 7-day development profile (measured) | 7 | 0.54 | 1.1 GB | measured 42 s (start-up dominated) | yes |
| 2016 Q1 (measured, "larger profile") | 91 | 6.94 | 13.9 GB | **measured 212 s** | yes |
| 2016 full year | 366 | ~27 | ~55 GB | ~14 min | yes |
| **All 731 days (brief's reference scope)** | 731 | 54.7 | 109 GB (+ prepared data < 0.1 GB, Hive/Spark scratch) | **~28 min** (projection, assumes linear scaling and no skew) | yes: 109 GB of 387.75 GB physical, leaving ~280 GB for staging copies/shuffle/logs |

Only the Hadoop Streaming stage scales with GDELT volume (Hive and Spark work on the 91-row daily audit table and the 619,040-row price table), so the projected full-workload run is about 28 min streaming + ~1 min Hive + ~5 min notebook, i.e. **~35 min against the 60-minute marking timeout** if the linear assumption holds. The projection is **not measured**; the main risk is mapper throughput (one Python process per map task; 4 vCores in total). Enhanced resource plan if the projection fails: more reducers and map-side parallelism first (`mapreduce.job.reduces`, split size), then additional core nodes only if the course allows exceeding the 1 primary + 2 core limit.

Decision to be recorded here with date, file list and checksums once confirmed.

## Prepared candidate larger profile — 2016 Q1

This is a locally acquired, checksum-verified candidate for the later full-profile
run; it is **not yet the declared assessed full profile**. It provides genuine input
growth relative to the seven-day development profile without exceeding the documented
HDFS capacity of the current AWS Academy cluster.

| Source | Dates / files | ZIP bytes | Expanded bytes | Event rows | Provenance |
|---|---|---:|---:|---:|---|
| GDELT 1.0 daily export | `20160101`–`20160331` (91 daily files) | 1,217,804,334 | 6,944,840,235 | 18,369,793 | `evidence/gdelt_manifest/q1_2016/manifest.tsv` (published MD5 = acquired MD5 for every file); `evidence/local/q1_2016/inventory.csv` (expanded-file SHA-256 and profile) |

Before running it on EMR, upload the extracted files to a separate versioned S3/HDFS
prefix (for example `raw/gdelt/v2026-10-29-q1-2016`) so the existing seven-day
development input remains reproducible. Copy the verified price CSV to the matching
`raw/prices/v2026-10-29-q1-2016/` prefix as well; Stage 2 deliberately uses one version
label for both sources. Use `evidence/local/q1_2016/streaming_gdelt_audit.tsv` as the
Stage 2 local comparison input for this profile.
