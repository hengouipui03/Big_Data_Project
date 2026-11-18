# Historical evidence imported from handover archive

The two subdirectories are byte-for-byte copies of the corresponding evidence directories in `Big_Data_Project.zip` (SHA-256 `bb9304df4e769e8eedd92d65abcb62a0b12957877d501e838173b40058e4073c`). They retain their original paths below `evidence/` and are separate from `evidence/final_run/`.

- `emr_run_2026-11-04_q1-2016/`: 91-day reference-profile shell logs, source inventories, notebook metrics and output checks. Its performance JSON records three notebook repetitions and one set of shell timings; its calculated end-to-end figure is not a median of three complete pipeline runs.
- `fresh_EMR_run_2026-11-14/`: additional notebook performance and reload records. Read each file's own timestamps and application IDs: the performance JSON is dated 8 November 2026 despite the directory name. Its shell and full end-to-end timing fields are null.

These historical records do not establish a final-run alternative-configuration comparison. The source ZIP also contains large duplicate prepared data, logs and Git internals; these were not copied into the curated project.
