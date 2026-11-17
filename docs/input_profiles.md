# Input profiles and performance

This submission follows the completed friend's handover: prices plus GDELT 2016 Q1. It does not incorporate the separate 731-day preparation workflow.

| Profile | GDELT coverage | Expanded GDELT bytes | Events | Evidence |
|---|---|---:|---:|---|
| Historical development | 2015-12-01 through 2015-12-07; 7 days | 536,722,649 | 1,425,234 | [local inventory](../evidence/local/inventory.csv), [EMR run log](../evidence/development/run_log.csv) |
| Submitted larger profile | 2016-01-01 through 2016-03-31; 91 days | 6,944,840,235 | 18,369,793 | [local inventory](../evidence/local/q1_2016/inventory.csv), [manifest](../evidence/gdelt_manifest/q1_2016/manifest.tsv), [HDFS inventory](../evidence/final_run/stage1_hdfs_inventory.csv) |

Both profiles use the same price snapshot: 29,580,549 bytes, 619,040 rows, SHA-256 `6aea253cd19de60b568143991aaf1fa482456565c389205658d236e595e716cf`. The Q1 compressed archives total 1,217,804,334 bytes. Acquisition provenance, checksums and published GDELT MD5s are retained in the inventories and consolidated manifest. Raw source files are excluded from delivery.

The final notebook/model output version is `v2026-15th-nov-2016`. Its actual shell evidence used older or unversioned raw paths. A fresh reproduction should stage the same exact input files under one chosen version as described in [RUN_ON_EMR.md](RUN_ON_EMR.md), and record the new version instead of changing historical evidence.

## Recorded timing

The final [shell run log](../evidence/final_run/run_log.csv) records Streaming 202 s, Hive registration 12 s, Hive transform 41 s and result queries 26 s: 281 s in total for those components. Staging was 97 s and is excluded from timed processing. The notebook reports a main-stage sum of 276.1 s and three prepare/final-fit benchmark repetitions of 46.62, 45.14 and 41.56 s (median 45.14 s). These quantities have different coverage and must be labelled separately.

The seven-day development Streaming stage took 42 s. This is genuine input growth, but one shell run at each size is not a median-of-three full-pipeline comparison. The final handover lacks the full timing/configuration comparison required for a stronger scalability claim. No unmeasured 731-day projection is presented as execution evidence. See [EVIDENCE_INDEX.md](EVIDENCE_INDEX.md) for all limitations and exact notebook cells.
