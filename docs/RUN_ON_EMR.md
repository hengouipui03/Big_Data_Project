# Appendix A source - EMR setup and rerun guide

*(This file is the working source of **Appendix A** of the report. Every `<ANGLE-BRACKET>` value is yours to replace. Never write access keys into any file, notebook or chat.)*

**What you will do**

```
 laptop: download raw data ──► S3 bucket  raw/prices/<VER>/  raw/gdelt/<VER>/
                                   │
 primary node: build numpy env ────┤ (once per bucket)          scripts/build_numpy_env.sh
 primary node: emr_run_all.sh ─────┤ Stage 1 S3→HDFS  +  Stage 2 Hadoop Streaming + Hive on Tez     ~5 min (+ ~2 min staging, excluded)
 JupyterHub: notebook "Run All" ───┤ PySpark cleaning · features · splits · Spark ML · analytics     ~5 min (+ 3 benchmark repetitions ~3 min)
 JupyterHub: restart kernel, run section 6 only ──► fresh-session reload proof                       ~1 min
                                   ▼
 outputs saved to S3 ──► download notebook + models + evidence ──► terminate the cluster
```

Order rule: **shell scripts first, notebook second, never both at the same time** (the notebook's Spark session holds the cluster's memory and Hadoop/Hive jobs would wait).
Everything on the cluster disk/HDFS disappears at termination; S3 does not - that is why the notebook writes its outputs to S3.

---

## 1. Prerequisites and cluster configuration

| Item | Value |
|---|---|
| Region | `us-east-1` |
| EMR release | `emr-7.10.0` (permitted by the course Q&A) |
| Applications | Hadoop 3.4.1, Hive 3.1.3 (execution engine **Tez**), Spark 3.5.5 (`3.5.5-amzn-1`), Livy 0.8.0, JupyterHub 1.5.0 (+ JupyterEnterpriseGateway 2.6.0). Python on the nodes: 3.9.x |
| Nodes | **1 primary + 2 core `m5.xlarge`**, no task nodes, no autoscaling (manual cluster size), manual termination |
| Root volume | 30 GiB per node (Q&A reference). *The 4 and 13 Nov clusters did not record this value in the exported metadata (`config/emr/EMR_cluster_history.md`); set it explicitly.* |
| Core data storage | Q&A reference: one 200 GiB gp3 volume per core node (3,000 IOPS, 125 MiB/s). **The 4 and 13 Nov clusters used two 100 GiB gp3 volumes per core node** (3,000 IOPS / 125 MiB/s each) = 200 GiB per node, so capacity matches but aggregate IOPS/throughput is double the reference; HDFS reported 387.75 GB configured capacity (`evidence/emr_run_2026-11-04_q1-2016/stage1_dfs_report.txt`). Primary: default 2 x 32 GiB gp2 |
| Software settings | paste `config/emr/cluster_configurations_reference_m5xlarge.json` into *Software settings → Enter configuration*, replacing `REPLACE-WITH-YOUR-BUCKET-NAME`. It sets HDFS replication 2, Spark 2 executors x 2 cores x 6 GiB heap + 2 GiB overhead, driver 2 GiB, dynamic allocation off, Hive on Tez, notebook persistence to S3 |
| Bootstrap actions | none |
| IAM | service role `EMR_DefaultRole`, instance profile `EMR_EC2_DefaultRole`, key pair `vockey` (AWS Academy defaults). The instance role needs read/write on your bucket |
| Cost | about USD 1 per hour while the cluster runs; the whole rerun takes under 1 hour of cluster time |
| Exact cluster exports of the two runs | `config/emr/metadata/nov04-*.json`, `nov13-*.json`; summary `config/emr/EMR_cluster_history.md` |

Resource names you must choose: **`<BUCKET>`** (an S3 bucket in us-east-1), **`<VER>`** = raw-data version folder, here `v2026-10-29-q1-2016`.

## 2. Prepare the environment

1. **Create the cluster** (AWS Academy → Start Lab → AWS console → EMR → Create cluster) with the table above. Note the cluster ID and time; wait for **Waiting**.
2. **Open ports** on the primary node's security group (inbound): SSH 22 and TCP 9443 from *My IP*.
3. **Log in:** `ssh -i <KEY>.pem hadoop@<PRIMARY-DNS>` (PowerShell: same command; if "permissions too open", restrict the .pem to your own user).
4. **Copy the project onto the primary node** (the cluster may not reach GitHub, so use the submission ZIP):
   ```bash
   # on your laptop
   scp -i <KEY>.pem submission.zip hadoop@<PRIMARY-DNS>:~
   # on the primary node
   sudo dnf install -y unzip   # only if unzip is missing
   unzip -q submission.zip -d project && cd project
   ```
   All commands below run from this folder. (If you prefer git: `git clone <your-repository-url>`.)
5. **Python dependency for the notebook (once per bucket).** PySpark's ML layer needs `numpy` on the Python workers; EMR's Python 3.9 has none. The notebook ships a packed virtual environment to the executors from S3:
   ```bash
   bash scripts/build_numpy_env.sh <BUCKET>      # ~2 min; uploads s3://<BUCKET>/dependencies/spark-numpy-env.tar.gz
   ```
   The notebook's first cell (`%%configure -f`) then sets `spark.yarn.dist.archives`, `spark.pyspark.python` and `spark.executorEnv.PYSPARK_PYTHON` to use it. **Edit the bucket name in that cell and in cell 0.1.** Without the archive the Spark session cannot start.
   Execution order of all helper files: `build_numpy_env.sh` → `emr_run_all.sh` (calls `stage1_stage_to_hdfs.sh`, then `stage2_streaming_and_hive.sh`, which call `src/streaming/gdelt_mapper.py` / `gdelt_reducer.py` and `sql/hive/01…03_*.hql`) → notebook.

## 3. Acquire and stage the data

**Sources and versions** (full provenance: `config/sources.json`, `evidence/local/acquisition_log.csv`, `evidence/gdelt_manifest/q1_2016/manifest.tsv`, data dictionary `docs/data_dictionary.md`):

| Source | Version / files | Expected size and count |
|---|---|---|
| Daily price CSV | `all_stocks_5yr.csv` (plotly/datasets, as linked in the brief) | 29,580,549 bytes, 619,040 rows + header, 505 tickers, 2013-02-08 … 2018-02-07; sha256 `6aea253cd19de60b568143991aaf1fa482456565c389205658d236e595e716cf` |
| GDELT 1.0 events | daily exports `20160101` … `20160331` (91 files, 2016 Q1) | 1,217,804,334 bytes zipped, 6,944,840,235 bytes expanded, 18,369,793 event rows; every file's md5 equals the published index |

**Download (laptop, Python 3.8+ and bash; no extra packages):**
```bash
python -I src/acquire/download_raw.py prices
bash scripts/acquire_gdelt.sh 20160101 20160331 data/raw/gdelt_q1_2016        # verifies md5 per day, writes evidence/gdelt_manifest/
python -I src/pipeline/t1_profile_raw.py data/raw evidence/local/q1_2016 config/sources.json data/raw/gdelt_q1_2016 evidence/gdelt_manifest/q1_2016/manifest.tsv   # inventory + quality summary (Task 1)
```
**Upload to S3 with exactly this layout** (and *nothing else* under `raw/` - an earlier staging also left unversioned copies, which doubled the HDFS inventory; see `evidence/emr_run_2026-11-04_q1-2016/stage1_hdfs_inventory_flat_copies_unused.csv`):
```
s3://<BUCKET>/raw/prices/<VER>/all_stocks_5yr.csv
s3://<BUCKET>/raw/gdelt/<VER>/20160101.export.CSV … 20160331.export.CSV     (unzipped files)
```
**Load into HDFS and create the Hive tables** - one command on the primary node (`emr_run_all.sh` Stage 1 = `hadoop distcp` S3 → `/raw/...` with bytes/lines/SHA-256 read back from HDFS; Stage 2 = Hadoop Streaming audit, then Hive external tables `inf2006.raw_prices`, `inf2006.gdelt_daily_audit`, then typed Parquet tables `inf2006.prices_typed`, `gdelt_daily_typed` that Spark reads through the Hive metastore):
```bash
bash scripts/emr_run_all.sh <BUCKET> <VER> evidence/emr_run_$(date -u +%F)
```
Done when it prints `ALL GOOD`. Expected: 92 files verified identical to the laptop SHA-256 (`stage1_hdfs_inventory.csv` vs `evidence/local/q1_2016/inventory.csv`), `RESULT: EMR streaming output matches local run on every date key`, Hive `rows_total=619040 tickers=505 rows_up_to_2017_12_29=605910 empty_open=11 empty_high=8 empty_low=8 ohlc_inconsistent=12 nonpositive=0 duplicate_ticker_date=0`.

## 4. Upload and execute the project

1. Browser → `https://<PRIMARY-DNS>:9443` (accept the certificate warning). Login: user `jovyan`, password `jupyter`. *Start My Server* if asked.
2. **Upload** `notebooks/inf2006_pipeline.ipynb`; open it; kernel must be **PySpark**.
3. Check the parameters: `VERSION = "<VER>"`, `BUCKET = "<BUCKET>"`, `ACTIVE_CONFIG = "reference"` (cell 0.1) and the bucket in the `%%configure` cell.
4. **Kernel → Restart Kernel and Run All Cells.** Sections run in this order: 0 setup · 1 corpus checks (Spark vs Hive vs local) · 2 cleaning, features, chronological splits, leakage checks · 3 model experiments (14 per task, validation only), frozen final fit, test, **3.0 training-session record**, baselines · 4 analytics Q1-Q3 + figures · 5 performance (3 repetitions, memory) · 6 reload · 7 verification record.
   *Section 6 inside "Run All" runs in the training session, so it records `fresh_session = False` and prints a note: that is expected - do step 9 for the real proof.*
5. **Expected outputs (2016-Q1 profile, reference configuration):**

| Section | Expected |
|---|---|
| 0.3 | `is_emr: true`, Spark `3.5.5-amzn-1`, 2 executors x 2 cores x 6g, new `application_…` id |
| 1 | all `PASS`; 619,040 raw rows; Hive = Spark = local Python |
| 2 | raw 619,040 = clean 605,887 + rejected 13,153; direction train/val/test = 66,424 / 25,805 / 25,533 rows; high-vol = 66,412 / 25,307 / 25,029; high-vol cut-off ≈ 0.01393; all `Leakage[...]` PASS |
| 3 | frozen choices: direction logistic regression/all_price, regParam 0; high-vol logistic regression/all_price, regParam 0.01. Test AUC direction **0.5001** (majority baseline 0.5000), high-vol **0.7261** (persistence baseline 0.6378) |
| 4 | Q1 table (4 groups), Q2 13 weekly points, Q3 14 subgroup rows + confusion matrices, 3 figures |
| 5 | main run ≈ 270 s (includes 2 x 14 experiments); 3 repetitions of prepare + final fit ≈ 47 s each; memory per executor |
| 7 | `34 of 34 checks passed` |

   **Approximate runtimes:** staging S3 → HDFS ≈ 2 min (excluded from the 60-minute run) · Stage 2 shell stages ≈ 285 s (Hadoop Streaming 212 s, Hive 15 + 33 + 25 s) · notebook main run ≈ 272 s · benchmark ≈ 3 x 47 s.
6. **Alternative configuration** (software-only comparison, identical inputs and checks): set `ACTIVE_CONFIG = "alternative"` in cell 0.1, Restart & Run All. Section 5.3 lists both configurations side by side.

## 5. Reload the saved model and verify results

1. **Models are written by section 3** to `hdfs:///models/<VER>/{direction,high_vol}` and `s3://<BUCKET>/notebook_outputs/<VER>/models/{direction,high_vol}` (complete `PipelineModel`: median Imputer → VectorAssembler → StandardScaler → LogisticRegression, learned parameters included).
2. **Fresh session (step 9 of the run):** *Kernel → Restart Kernel* (do **not** run all), then run only the cell of **section 6** - it is self-contained. It starts a new Spark application, loads both models **without retraining**, re-scores the 2017 test set and compares with the predictions saved at training time (`pred_<task>`).
3. **Expected result:** `fresh session : True` (reload `application_id` ≠ training `application_id` in `training_session.json`), all `PASS`, `RELOAD VERDICT: ALL PASSED`, which means for each task and each of the two model locations (HDFS, S3): metadata + stages present; row counts equal (25,533 direction / 25,029 high-vol); max |probability difference| = 0.0 (tolerance 1e-9); 0 differing class predictions; **test AUC and accuracy re-computed from the loaded model equal the training-session results within 1e-6** (direction 0.50006 / 0.56311, high-vol 0.72608 / 0.77522).
4. **Dated evidence of the team's own fresh-session reruns** (all from the same code; the metric re-computation check in item 3 was added after these runs):
   * 2026-11-13 cluster `j-0116410E858ST6KQS49`: training session `application_1794579383226_0007`, reload session `application_1794579383226_0008` → `evidence/fresh_EMR_run_2026-11-14/reference/training_session.json`, `t3_reload.json` (`all_checks_passed: true`, max difference 0.0).
   * 2026-11-04 cluster `j-06878942C5H7E0OZ24GA` (earlier run, reload in the same session as training - **not** a fresh-session proof) → `evidence/emr_run_2026-11-04_q1-2016/notebook_outputs/`.
   * *Final rehearsal run (with the metric re-computation check): to be added after the next cluster run.*

## 6. Troubleshooting and cleanup

| Symptom | Cause → fix |
|---|---|
| `Permission denied (publickey)` | use `hadoop@`, the `.pem` of the current lab session, restrict its file permissions |
| ssh / JupyterHub time out | the security group lacks your current IP (step 2.2); JupyterHub is `https://…:9443` |
| Cluster creation `VALIDATION_ERROR` | check EBS settings (volume type/size/count) and that you picked the AWS Academy roles |
| `No route to any external sources` | choose a public subnet and clone the cluster |
| Notebook cell 1 (`%%configure`) / session never starts | the numpy archive is missing or the bucket name is wrong → rerun `build_numpy_env.sh`, check `aws s3 ls s3://<BUCKET>/dependencies/`; check Livy log: *Application user interfaces → YARN → application → logs* |
| `ModuleNotFoundError: numpy` in a stage | executors are not using `./environment/bin/python`; check the three keys in the `%%configure` cell |
| `emr_run_all.sh`: `/raw/prices/<VER> is missing` | S3 version folder name differs: `aws s3 ls s3://<BUCKET>/raw/prices/` |
| Stage 1 `AccessDenied` / `NoSuchBucket` | bucket typo or the EC2 instance role cannot read it |
| A Hadoop/Hive step never progresses | a notebook session holds the memory: `yarn application -list`, then `yarn application -kill <application_…>` (`emr_run_all.sh` does this automatically at the start) |
| Stage 2 failed after streaming succeeded | resume: `FROM=c bash scripts/stage2_streaming_and_hive.sh <VER> <evidence-dir> 2 evidence/local/q1_2016/streaming_gdelt_audit.tsv` (a=streaming b=register c=transform d=results) |
| `hive: command not found` | `HIVE_CMD="beeline -u jdbc:hive2://localhost:10000 -n hadoop --silent=true" bash scripts/stage2_streaming_and_hive.sh …` |
| Notebook: *Hive tables not found* | Stage 2 was not run on **this** cluster (the metastore dies with the cluster) → run `emr_run_all.sh`, then the notebook from the top |
| Notebook: *No writable output location* | the instance role cannot write to the bucket; the notebook falls back to `hdfs:///project/notebook_outputs/<VER>` - copy results out with `hdfs dfs -get` **before** terminating |
| `Session … has been dead` | the Livy session was killed (idle timeout / memory); restart the kernel and Run All (~10 min) |
| `matplotlib` missing in figure cells | in a `%%local` cell: `!pip install --user matplotlib`, rerun the figure cells |
| Section 6 says `fresh session : False` | you ran it in the training session; restart the kernel and run only that cell |

**Preserve outputs, then clean up (do this before terminating):**
1. JupyterHub → *File → Download as → Notebook (.ipynb)*; save as `notebooks/inf2006_pipeline.ipynb` (this executed export goes in the ZIP; a copy also persists under `s3://<BUCKET>/jupyter/`).
2. Models, evidence JSON, timings are already in S3. Pull them to the primary node and then to your laptop:
   ```bash
   aws s3 sync s3://<BUCKET>/notebook_outputs/<VER> ~/outputs                         # on the primary node
   scp -i <KEY>.pem -r hadoop@<PRIMARY-DNS>:~/outputs ./outputs_<VER>                 # on your laptop
   scp -i <KEY>.pem -r hadoop@<PRIMARY-DNS>:~/project/evidence/emr_run_<DATE> ./       # shell-stage evidence
   ```
   Place the models under `models/<VER>/{direction,high_vol}/` (**check that each `stages/*/data/` folder came along** - the repository's `.gitignore` once hid them) and the evidence JSON under `evidence/<run-folder>/`.
3. Take dated screenshots (cluster *Waiting* with ID, notebook sections 0.3, 5, 6, 7) - screenshots support but never replace the evidence files.
4. Save cluster metadata: `aws emr describe-cluster --cluster-id <j-…> > config/emr/metadata/<date>-cluster.json` (and `list-instance-groups`).
5. **Terminate:** EMR console → select the cluster → *Terminate*; wait for *Terminated*. Optionally delete `s3://<BUCKET>/notebook_outputs/` and `dependencies/` and any extra EBS/security-group rules to avoid charges.

---

## Reproducibility record

| Item | Value |
|---|---|
| EMR release / software | `emr-7.10.0`; Hadoop 3.4.1, Hive 3.1.3 on Tez, Spark 3.5.5-amzn-1, Livy 0.8.0, JupyterHub 1.5.0; Python 3.9.25 (+ numpy, `scripts/build_numpy_env.sh`) |
| Raw data version | `v2026-10-29-q1-2016`: prices `all_stocks_5yr.csv` (sha256 `6aea253c…716cf`) + GDELT 1.0 `20160101`-`20160331` (91 days, 6,944,840,235 bytes) |
| Training cutoff | training anchors ≤ 2015-12-31 with outcomes known by that date (last known 2015-12-28); validation 2016; test 2017 (≤ 2017-12-29) |
| Random seed | 42 (random forest / GBT seeds; logistic regression is deterministic) |
| Tolerances | predictions: 1e-9 on probabilities; re-computed AUC/accuracy: 1e-6 |
| Saved models | `hdfs:///models/<VER>/{direction,high_vol}`; `s3://<BUCKET>/notebook_outputs/<VER>/models/{direction,high_vol}`; submission copy `models/<VER>/{direction,high_vol}/` |
| Assessed clusters | 2026-11-04 `j-06878942C5H7E0OZ24GA`; 2026-11-13/14 `j-0116410E858ST6KQS49` (both `config/emr/EMR_cluster_history.md`) |
| Final rehearsal cluster / date | *to be filled after the next run* |
