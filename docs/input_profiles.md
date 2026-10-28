# Input profiles (brief v1.1, Task 1.1: "Freeze and document your selected development and full input profiles before comparing configurations")

Each profile is an exact list of raw files identified by sha256, so any run can be matched to its inputs
(brief: "identical input manifests").

## Development profile — FROZEN 2026-10-28

| Source | Files | Expanded bytes | Rows | Provenance |
|---|---|---|---|---|
| Daily price CSV | `all_stocks_5yr.csv` | 29,580,549 | 619,040 (+1 header) | `evidence/local/acquisition_log.csv`, sha256 `6aea253c…716cf` |
| GDELT 1.0 daily export | `20151201`–`20151207.export.CSV` (7 days) | 536,722,649 (zips: ~140 MB) | 1,425,234 events | `evidence/local/acquisition_log.csv` (zip sha256), `evidence/local/inventory.csv` (expanded sha256) |

HDFS layout: `/raw/prices/v2026-10-27/`, `/raw/gdelt/v2026-10-27/`. S3 mirror: `s3://<bucket>/raw/...`.
HDFS verification of every file: `evidence/emr_ref_m5xlarge/stage1_hdfs_inventory.csv` (all 8 sha256 equal the laptop values).

Why these 7 days: a contiguous week inside the GDELT window (Jul 2015–Jun 2017) that overlaps the price
data, small enough (0.5 GB) to test every pipeline stage in minutes on the reference cluster.

## Full profile — NOT YET FROZEN

Brief v1.1: "Any common assessed workload will be confirmed by the teaching team before full runs"
and "develop on a small sample until then". The full profile will be frozen here once the teaching team
confirms the assessed workload. Candidate: prices (same file) + a contiguous GDELT range of N days.

Sizing evidence from the dev profile (reference cluster, default storage):
- Streaming audit: 42 s for 0.51 GB expanded (≈ 80 s/GB) — `evidence/emr_ref_m5xlarge/run_log.csv`
- HDFS: 115.75 GB physical across 2 workers ≈ 58 GB logical at replication 2 (default storage; the
  200 GiB gp3 reference volume was rejected by AWS Academy) — `stage1_dfs_report.txt`
- GDELT expanded size ≈ 75 MB/day average (54.71 GB / 731 days); zip ≈ 13 MB/day
- Budget rule (brief): raw + prepared files + simultaneous copies + shuffle + temp + logs must fit the
  logical ceiling; compressed and expanded sizes are not interchangeable.

| Candidate full range | Days | Expanded GB | ×2 replication | Est. streaming time |
|---|---|---|---|---|
| 2016 Q1 | 91 | ~6.8 | ~13.6 GB | ~9 min |
| 2016 H1 | 182 | ~13.6 | ~27 GB | ~18 min |
| 2016 full year | 366 | ~27 | ~55 GB (exceeds 58 GB logical ceiling with working space) | ~36 min |
| All 731 days | 731 | 54.7 | 109 GB — does not fit default storage | ~73 min |

Decision to be recorded here with date, file list and checksums once confirmed.
