# Run the whole project on AWS EMR — idiot-proof guide

*(This is the working draft of **Appendix A: EMR setup and rerun guide**.)*

**What you will do, in one picture**

```
laptop ── git push ──► GitHub ──► (cluster) git clone
                                      │
   S3 bucket (raw data) ──► [emr_run_all.sh]  Stage 1 S3→HDFS  +  Stage 2 Hadoop Streaming + Hive      ~10 min
                                      │
                         JupyterHub ► notebook "Run All"  (PySpark, Spark ML, analytics, timings)       ~35–50 min
                                      │
          outputs saved to S3  ◄──────┘  → download .ipynb + models + evidence  → terminate cluster
```

**Total hands-on time:** about 25 minutes of clicking and typing, plus about 1 hour of waiting (cluster start 10–15 min, scripts ~10 min, notebook ~40 min).

**Rules that prevent 90 % of problems**

1. Copy-paste commands exactly. Anything in `<ANGLE BRACKETS>` is yours to replace.
2. **Never run the notebook and the shell scripts at the same time.** The notebook holds the cluster's memory (`2 executors × 8 GiB`); Hadoop/Hive jobs then wait forever. Order: **scripts first, notebook second.**
3. Everything on the cluster's disk and HDFS **disappears when you terminate it**. S3 does not. The notebook saves its outputs to S3 for this reason.
4. AWS Academy ends your session after ~4 h and kills the cluster. Start the clock when you start the cluster.
5. Never put access keys in any file, notebook or chat.

---

## Step 0 — before touching AWS (laptop, 5 min)

1. Make sure everything is pushed so the cluster can clone it:
   ```bash
   cd Big_Data_Project
   git add notebooks scripts docs src sql config evidence/local
   git commit -m "Notebook, scripts, docs"
   git push
   ```
2. Check the raw data is already in S3 (AWS Console → S3 → your bucket, here `inf2006-team07-bigdata`). You must see:
   ```
   raw/prices/v2026-10-27/all_stocks_5yr.csv                 (29,580,549 bytes)
   raw/gdelt/v2026-10-27/20151201.export.CSV … 20151207.export.CSV   (7 files, 536,722,649 bytes in total)
   ```
   If not: upload them (unzipped GDELT files!) following `docs/stage1_emr_setup.md` section A.
3. Have `labsuser.pem` downloaded (AWS Academy → AWS Details → Download PEM).

## Step 1 — start AWS Academy and open the console

AWS Academy → your course → **Learner Lab → Start Lab** → wait for the green dot → click **AWS**. Region: **us-east-1**.

## Step 2 — create the cluster (10 min of clicking, then 10–15 min of waiting)

EMR → **Create cluster**. Settings (identical to `docs/stage1_emr_setup.md` section B):

| Field | Value |
|---|---|
| Name | `inf2006-bigdata` |
| Release | `emr-7.10.0` |
| Applications | tick **Hadoop, Hive, Tez, Spark, Livy, JupyterHub** (Livy is required by the PySpark notebook) |
| Primary | m5.xlarge × 1 |
| Core | m5.xlarge × 2 — **do NOT add a custom EBS volume** (AWS Academy rejects it; leave the default) |
| Task group | remove it. No scaling. |
| Termination | manual |
| Software settings | "Enter configuration" → paste `config/emr/cluster_configurations_reference_m5xlarge.json`, replacing `REPLACE-WITH-YOUR-BUCKET-NAME` with `inf2006-team07-bigdata` |
| Logs | `s3://inf2006-team07-bigdata/emr-logs/` |
| Key pair | `vockey` |
| IAM | service role `EMR_DefaultRole`, instance profile `EMR_EC2_DefaultRole` |

Click **Create cluster**. **Write down the Cluster ID (`j-…`) and the time.** Wait until the status is **Waiting** (green). Cluster page → copy the **Primary node public DNS** (`ec2-…compute-1.amazonaws.com`).

> If creation fails with *VALIDATION_ERROR*: you added a custom EBS volume. Clone the cluster and remove it.
> If it fails with *No route to any external sources*: pick a public subnet (see Troubleshooting).

## Step 3 — open the doors (2 min)

Cluster page → **Network and security → Primary node security group → Edit inbound rules → Add rule** (twice):

| Type | Port | Source |
|---|---|---|
| SSH | 22 | My IP |
| Custom TCP | 9443 | My IP |

(If your IP changes — e.g. you move Wi-Fi — redo this with the new "My IP".)

## Step 4 — log in to the primary node (laptop terminal)

```bash
chmod 400 ~/Downloads/labsuser.pem
ssh -i ~/Downloads/labsuser.pem hadoop@<PRIMARY-DNS>
```
*Windows PowerShell:* `ssh -i $HOME\Downloads\labsuser.pem hadoop@<PRIMARY-DNS>` (if "permissions too open", right-click the .pem → Properties → Security → keep only yourself).

The prompt becomes `[hadoop@ip-… ~]$` — you are now on the cluster. Answer `yes` to the fingerprint question.

## Step 5 — get the code onto the cluster

```bash
sudo dnf install -y git
git clone https://github.com/hengouipui03/Big_Data_Project.git
cd Big_Data_Project
```
(Already cloned from an earlier cluster? Not possible — the disk was wiped. Always clone.)

## Step 6 — run Stages 1 and 2 with ONE command (~10 min)

```bash
bash scripts/emr_run_all.sh inf2006-team07-bigdata v2026-10-27
```

What it does, in order (and prints a heading for each): stops leftover Spark sessions → copies S3→HDFS and verifies every file's SHA-256 (**Stage 1**, ~1 min) → runs the **Hadoop Streaming** audit job (~1–2 min) → runs the **Hive** scripts on Tez (~3–5 min; the first Tez start is slow) → copies `run_log.csv` to HDFS → checks the Hive tables → copies the evidence to S3.

**You are done with this step when you see a green line:** `ALL GOOD. Now open JupyterHub …`

Expected values it checks (so you can compare with the screen):

| What | Expected |
|---|---|
| `/raw/prices/v2026-10-27/all_stocks_5yr.csv` | 29,580,549 bytes; 619,041 lines (619,040 rows + header) |
| `stage2_streaming_vs_local.txt` last line | `RESULT: EMR streaming output matches local run on every date key` |
| `inf2006.prices_typed` | 619040 rows |
| `inf2006.gdelt_daily_typed` | 7 days (dev profile) |

If it stops with a red `STOP:` message, read the message — it says what to do. More in Troubleshooting.

## Step 7 — open JupyterHub (2 min)

Browser: `https://<PRIMARY-DNS>:9443` → ignore the certificate warning ("Advanced → Proceed").
Login: user **`jovyan`**, password **`jupyter`**. Click **Start My Server** if asked.

## Step 8 — upload and run the notebook (~35–50 min of waiting)

1. In the JupyterHub file browser click **Upload** (up arrow) → choose `notebooks/inf2006_pipeline.ipynb` from your laptop (the copy in your git clone).
2. Open it. Top right must say **PySpark**. If it says another kernel: *Kernel → Change kernel → PySpark*.
3. Look at the cell titled **0.1 Parameters**: `VERSION = "v2026-10-27"` and `BUCKET = "inf2006-team07-bigdata"` must be what you used in Step 6. `ACTIVE_CONFIG = "reference"` for the first run.
4. **Kernel → Restart Kernel and Run All Cells.**
5. Walk away. The first cell starts the Spark session (1–2 min), then each section prints progress. Expected total: dev profile **~30–50 min** (the 3-repetition benchmark in Section 5 is the longest part).

What good looks like:

| Section | You should see |
|---|---|
| 0.3 Environment | `is_emr: true`, Spark `3.5.5-amzn-1`, an `application_…` id, 2 executors × 2 cores × 6g |
| 1 Corpus | `PASS` lines; prices rows 619040; Hive vs Spark numbers equal |
| 2 Pipeline | `raw = clean + rejected` PASS; clean rows **605,887**; split table; all `Leakage[…]` lines PASS |
| 3 Models | 14 experiment lines per task; "frozen choice"; a test table comparing model vs baselines |
| 4 Analytics | tables + 3 figures (Q2 says INSUFFICIENT COVERAGE on the 7-day profile — that is correct and honest) |
| 5 Performance | stage timings, 3 repetitions, medians, memory per executor |
| 6 Reload | `RELOAD VERDICT: ALL PASSED` |
| 7 Verification | "N of N checks passed" |

If a figure cell prints `Plot skipped`, run this once in a new cell and rerun that cell:
```
%%local
!pip install --user matplotlib
```
A failed check (`FAIL`) is **not** a reason to hide it — it goes in the report as a failed check with the actual value.

## Step 9 — fresh-session reload proof (3 min)

1. *Kernel → Restart Kernel* (do not run all).
2. Run only the cell titled **6. Saved-model reload** (it is self-contained).
3. Expected: `fresh session : True`, all `PASS`, `RELOAD VERDICT: ALL PASSED`. Take a screenshot with the date visible.

## Step 10 — second configuration and (when the teaching team confirms it) the full profile

* **Alternative configuration** (software-only comparison): in cell 0.1 change `ACTIVE_CONFIG = "alternative"` → Restart & Run All. Section 5.3 then lists reference and alternative side by side. Same inputs, same checks.
* **Full profile:** acquire the larger GDELT range (`scripts/acquire_gdelt.sh`, `docs/input_profiles.md`), upload it to `s3://<bucket>/raw/{prices,gdelt}/<new-version>/`, run Step 6 with the new version name (`bash scripts/emr_run_all.sh <bucket> <new-version>`), change `VERSION` in cell 0.1 to the same name, and run the notebook again (both configurations). Do not run two configurations at once.

## Step 11 — save everything BEFORE terminating

1. **Notebook with outputs:** JupyterHub → *File → Download → Notebook (.ipynb)*. Save it as `notebooks/inf2006_pipeline.ipynb` (this executed copy is what goes in the ZIP; it is also in `s3://<bucket>/jupyter/jovyan/`).
2. **Models, evidence JSON, timings** are already in S3:
   ```
   s3://inf2006-team07-bigdata/notebook_outputs/<VERSION>/models/direction/
   s3://inf2006-team07-bigdata/notebook_outputs/<VERSION>/models/high_vol/
   s3://inf2006-team07-bigdata/notebook_outputs/<VERSION>/evidence/
   ```
   Copy them to the primary node and then to your laptop:
   ```bash
   # on the primary node
   aws s3 sync s3://inf2006-team07-bigdata/notebook_outputs/<VERSION> ~/outputs
   # on your LAPTOP (new terminal)
   scp -i ~/Downloads/labsuser.pem -r hadoop@<PRIMARY-DNS>:~/outputs ./outputs_<VERSION>
   scp -i ~/Downloads/labsuser.pem -r hadoop@<PRIMARY-DNS>:~/Big_Data_Project/evidence/emr_ref_m5xlarge ./evidence_emr
   ```
   Put `models/` under `models/` in the submission folder, the evidence under `evidence/`, and commit the small evidence files (not the data).
3. Take screenshots: EMR cluster page (Waiting, with ID), notebook Section 0.3, 5, 6 and 7 outputs, with the date visible.

## Step 12 — terminate (stops the billing)

EMR console → select the cluster → **Terminate**. Wait for **Terminated**. (~USD 1 per hour while running.)

---

## What to put in the ZIP (paths relative to the ZIP root)

| File | Meaning |
|---|---|
| `report.pdf` | the report with Appendices A–D |
| `notebooks/inf2006_pipeline.ipynb` | **the executed copy exported from EMR (Step 11.1)** |
| `models/direction/`, `models/high_vol/` | saved `PipelineModel`s |
| `scripts/*.sh`, `src/streaming/*.py`, `sql/hive/*.hql`, `config/**` | helper scripts and configuration |
| `evidence/emr_ref_m5xlarge/` | Stage 1–2 logs, `run_log.csv`, screenshots |
| `evidence/notebook/` | the JSON files from `notebook_outputs/<VERSION>/evidence/` (verification record, reload, performance) |
| `docs/RUN_ON_EMR.md` | this guide (Appendix A) |

No credentials, no raw data (`data/`, the zips), no machine-specific paths.

---

## Troubleshooting

| Symptom | Cause → fix |
|---|---|
| `Permission denied (publickey)` on ssh | wrong user or key: use `hadoop@`, the `labsuser.pem` of the *current* lab session, and `chmod 400` |
| ssh / browser times out | the security group has no rule for your current IP (Step 3) |
| JupyterHub login fails | user `jovyan`, password `jupyter`; use `https://` and port **9443** |
| Cluster creation: *VALIDATION_ERROR* | custom EBS volume — remove it (Academy rejects it) |
| Cluster creation: *No route to any external sources* | private subnet — clone the cluster and choose a public subnet |
| `emr_run_all.sh`: `/raw/prices/<version> is missing` | the version folder name differs from S3. `aws s3 ls s3://<bucket>/raw/prices/` shows the real names |
| Stage 1: `AccessDenied` / `NoSuchBucket` | bucket name typo; or the EMR role cannot read it — recheck IAM roles (Step 2) |
| A Hadoop/Hive step sits at "Running job" and never moves | a notebook session holds the memory. `emr_run_all.sh` kills it automatically; manually: `yarn application -list` then `yarn application -kill <application_…>` |
| Stage 2 failed after streaming succeeded | resume: `FROM=c bash scripts/stage2_streaming_and_hive.sh <version> evidence/emr_ref_m5xlarge 2 evidence/local/streaming_gdelt_audit.tsv` |
| `hive: command not found` | `HIVE_CMD="beeline -u jdbc:hive2://localhost:10000 -n hadoop --silent=true" bash scripts/stage2_streaming_and_hive.sh …` |
| Notebook cell 1: *Hive tables … not found* | Step 6 was not run on **this** cluster (the Hive metastore dies with the cluster). Run `emr_run_all.sh` again, then re-run the notebook from the top |
| Notebook: *No writable output location* | the EMR instance role cannot write to the bucket. The notebook already falls back to HDFS (`hdfs:///project/notebook_outputs/<VERSION>`); copy results out with `hdfs dfs -get` before terminating |
| `Session … has been dead` / kernel dies mid-run | the Livy session was killed (idle timeout or out of memory). Restart the kernel and *Run All* (all outputs are recreated; ~40 min) |
| A cell is very slow | normal for the 3-repetition benchmark (Section 5). Check progress in the Spark UI: Cluster page → Application user interfaces |
| `matplotlib` missing in figure cells | `%%local` cell with `!pip install --user matplotlib`, rerun the figure cell |
| Q2 says INSUFFICIENT COVERAGE | correct on the 7-day profile; needs the larger GDELT range (Step 10) |
| Everything vanished | the cluster was terminated (Academy session ended). Create a new cluster (Clone), repeat from Step 3 |

## Reproducibility record (fill in after your run — Appendix A item 5)

| Item | Value |
|---|---|
| EMR release | `emr-7.10.0` (Spark 3.5.5-amzn-1, Hive on Tez) |
| Raw data version | `v2026-10-27` (7 GDELT days 2015-12-01..07; prices `all_stocks_5yr.csv`, sha256 `6aea253c…716cf`) |
| Training cutoff | training anchors ≤ 2015-12-31 (outcomes known by that date); validation 2016; test 2017 (≤ 2017-12-29) |
| Random seed | 42 (logistic regression is deterministic) |
| Saved models | `s3://<bucket>/notebook_outputs/<VERSION>/models/{direction,high_vol}` and `hdfs:///models/<VERSION>/` |
| Cluster ID / date of the assessed run | _fill in_ |
| Date of the fresh-session rerun | _fill in_ (notebook Section 6 output + screenshot) |
