# Stage 1 — EMR cluster and raw corpus in HDFS

Goal: establish a running EMR cluster with raw files in HDFS and preserve dated proof of that run.
This document is an **Appendix A** (EMR setup and rerun guide) working draft populated with the
historical evidence from the successful 2026-10-28 Stage 1 run.

**AWS Academy finding (2026-10-28):** The 200 GiB gp3 EBS volume in the provisional
reference configuration was rejected by the AWS Academy Learner Lab account with a permissions /
`VALIDATION_ERROR` (failed cluster `j-06371601XD1JEGSGGUW6`). The successful m5.xlarge run used
EMR **default gp2 storage**, reported as two 32 GiB volumes per core node. This is a **hardware/storage
deviation**, not a verified match to the reference: gp2 capacity and performance are not equivalent
to the specified 200 GiB gp3 volume at 3000 IOPS and 125 MiB/s. The small development input fits
on the observed HDFS capacity; do not claim that the full reference hardware requirement or an
assessed full-profile run has been met. The project notes say this issue was raised with teaching
staff; the brief itself is not included in this repository ZIP. Evidence is under
`evidence/emr_ref_m5xlarge/`.

## Recorded setup from the successful Stage 1 run (28 October 2026)

These are **historical run details**, not current running-cluster information. The recorded cluster
was terminated at **2026-10-28 07:58 UTC**; recreate it before a new run. Values marked *documented*
are setup instructions rather than an independently verified AWS-console property; the region is
inferred from the successful node hostname and should be checked in the console.

| Item | Recorded value | Where the value comes from |
|---|---|---|
| Profile | Reference **m5.xlarge** (1 primary + 2 core, no task; not m5.large dev) | `config/emr/README.md` lines 6–14; `evidence/emr_ref_m5xlarge/stage1_cluster.txt` lines 10–14 |
| AWS account | AWS Academy Learner Lab | `config/emr/README.md` lines 12, 19–20; original setup notes in this document |
| EMR release | `emr-7.10.0` | `evidence/emr_ref_m5xlarge/stage1_cluster.txt` line 2 |
| Core-node storage | EMR default: **2 × 32 GiB gp2 per core**, not the reference 200 GiB gp3 | `evidence/emr_ref_m5xlarge/stage1_notes.txt` line 7; `config/emr/README.md` line 12 |
| AWS region | **`us-east-1` (consistent with the recorded primary-node DNS; verify the region in the AWS Console for any new run)** | `evidence/emr_ref_m5xlarge/stage1_notes.txt` line 8 records a `compute-1.amazonaws.com` hostname |
| S3 bucket name | `inf2006-team07-bigdata` | `evidence/emr_ref_m5xlarge/stage1_cluster.txt` line 3; `stage1_distcp.log` line 1 |
| Cluster ID | `j-02025102D1KMIV42E97Q` | `evidence/emr_ref_m5xlarge/stage1_cluster.txt` line 1; `stage1_notes.txt` line 2 |
| Cluster created (UTC) | `2026-10-28 06:02 UTC` | `evidence/emr_ref_m5xlarge/stage1_notes.txt` line 5 |
| Cluster **Waiting** (UTC) | `2026-10-28 06:09 UTC` | `evidence/emr_ref_m5xlarge/stage1_notes.txt` line 6 |
| Primary node public DNS | `ec2-52-91-240-125.compute-1.amazonaws.com` | `evidence/emr_ref_m5xlarge/stage1_notes.txt` line 8 |
| Key pair name | `vockey` (**documented setup; console verification not saved**) | `config/emr/README.md` line 20; setup instructions in section B below |
| Staging start (UTC) | `2026-10-28 06:14:18 UTC` | `evidence/emr_ref_m5xlarge/stage1_cluster.txt` line 6 |
| Staging end (UTC) | `2026-10-28 06:15:02 UTC` | `evidence/emr_ref_m5xlarge/stage1_cluster.txt` line 7 |
| Staging duration (excluded from assessed 60 minutes) | **44 seconds** | `evidence/emr_ref_m5xlarge/stage1_cluster.txt` line 8; `stage1_notes.txt` line 10 |
| DistCp YARN application ID | `application_1793167721813_0001` | `evidence/emr_ref_m5xlarge/stage1_cluster.txt` line 5; `stage1_distcp.log` line 25 |
| HDFS replication | **2** | `evidence/emr_ref_m5xlarge/stage1_cluster.txt` line 9; `config/emr/cluster_configurations_reference_m5xlarge.json` lines 2–6 |
| Two core nodes running | **2 × RUNNING** when tested | `evidence/emr_ref_m5xlarge/stage1_cluster.txt` lines 10–14 |
| Cluster terminated (UTC) | `2026-10-28 07:58 UTC` | `evidence/emr_ref_m5xlarge/stage1_notes.txt` line 13 |

### Verified outcomes and input traceability

- **S3 → HDFS DistCp succeeded:** 8 raw files; **566,303,198 bytes copied**; **44 s** wall-clock staging.
  Check `evidence/emr_ref_m5xlarge/stage1_distcp.log` lines 40–41, 49–56, and 86–91.
- **Inputs in HDFS:** 1 price file at `/raw/prices/v2026-10-27/all_stocks_5yr.csv`
  (**29,580,549 bytes**, **619,041 lines including header**), plus seven unzipped GDELT files dated
  2015-12-01 through 2015-12-07 under `/raw/gdelt/v2026-10-27/`
  (**536,722,649 bytes**, **1,425,234 event lines**). See
  `evidence/emr_ref_m5xlarge/stage1_hdfs_inventory.csv` lines 2–9 and
  `docs/input_profiles.md` lines 6–17.
- **Byte-for-byte validation:** all **8 of 8** staged HDFS files have the **same size and SHA-256**
  as the downloaded/unzipped files listed in `evidence/local/inventory.csv` (lines 2–9).
  The HDFS inventory is `evidence/emr_ref_m5xlarge/stage1_hdfs_inventory.csv` (lines 2–9).
  The original download URLs and acquisition timestamps are in
  `evidence/local/acquisition_log.csv` (lines 2–9) and `config/sources.json`.
- **Capacity/health:** configured HDFS capacity **124,283,478,016 bytes (115.75 GiB)**,
  used **1,190,788,240 bytes (1.11 GiB)** at the recorded check, with **zero under-replicated,
  missing, or corrupt blocks**. See `evidence/emr_ref_m5xlarge/stage1_dfs_report.txt` lines 1–11.
  This is a snapshot for the small development corpus, **not** proof that the complete workload fits.
- **Jupyter/PySpark smoke test:** the screenshot
  `evidence/emr_ref_m5xlarge/screenshots/06_notebook_first_cell.png` shows Spark
  `3.5.5-amzn-1`, application `application_1793167721813_0002`, and a text-line count of
  **619,041** for the HDFS price CSV; the application ID is also noted in `stage1_notes.txt` line 12.
- **Screenshots:** Under `evidence/emr_ref_m5xlarge/screenshots/`, `03_failed_cluster_ebs_error.png`
  documents the rejected EBS setup; `04_staging_done.png`, `05_hdfs_inventory.png`, and
  `06_notebook_first_cell.png` document the successful processing. `02_instance_groups.png`
  shows the one-primary/two-core group layout. The cluster creation, Waiting and termination
  timestamps are recorded in `stage1_notes.txt`; the DistCp and YARN logs substantiate the
  data-staging results.

### Still requiring evidence or confirmation

The ZIP does **not** include the course brief itself or a complete EMR configuration export
independently establishing the cluster's **region, SSH key pair, S3 bucket configuration and
security-group settings**. The guide below records the intended settings, but some values remain
documented rather than independently confirmed in the saved logs. The teaching team's acceptance
of the gp2-vs-gp3 deviation and the **full input profile** also remain unconfirmed
(`docs/input_profiles.md` lines 19–40). For a future rerun, save the cluster configuration/region,
instance storage and key pair, S3 object listing, security-group rules and new dated result set.

---

## A. One-time: S3 bucket and raw data upload (done from your laptop)

S3 is the permanent home of the raw corpus. HDFS is wiped when the cluster is terminated, so every
rerun starts by copying S3 -> HDFS. Download and S3 -> HDFS copy are excluded from the 60-minute run
but the copy time must be recorded separately.

1. AWS console -> S3 -> Create bucket. Name must be globally unique, e.g. `inf2006-teamXX-bigdata`.
   Keep all defaults (block public access ON). Note the region.
2. Create this folder layout and upload (console: Upload -> Add files / Add folder):

```
s3://<bucket>/raw/prices/v2026-10-27/all_stocks_5yr.csv
s3://<bucket>/raw/gdelt/v2026-10-27/20151201.export.CSV
s3://<bucket>/raw/gdelt/v2026-10-27/...            (one file per downloaded day)
```

   `v2026-10-27` = the acquisition date from `evidence/local/acquisition_log.csv`. This is how the
   corpus "distinguishes sources, versions and acquisition dates" (brief, 1.2). Upload the UNZIPPED
   GDELT files: Hadoop reads plain text in parallel but cannot split a .zip.

   Alternative with the AWS CLI (requires valid lab credentials configured locally;
   never commit them):

   ```bash
   aws s3 sync data/raw/prices/ s3://<bucket>/raw/prices/v2026-10-27/
   aws s3 sync data/raw/gdelt/ s3://<bucket>/raw/gdelt/v2026-10-27/ --exclude "*.zip"
   ```

   These commands create the versioned layout expected by the staging and Hive scripts.

3. Verify in the console that the object sizes equal the `bytes` column of `evidence/local/inventory.csv`.

## B. Create the cluster

AWS console -> EMR -> Create cluster.

1. **Name**: `inf2006-bigdata`. **Release**: `emr-7.10.0`.
2. **Applications**: tick Hadoop, Hive, Tez, Spark, Livy, JupyterHub. (Livy is required by JupyterHub's
   PySpark kernel.)
3. **Cluster configuration** -> Instance groups:
   - Primary: m5.xlarge, 1 instance.
   - Core: m5.xlarge, 2 instances. Do NOT add a custom EBS volume in AWS Academy (it is rejected);
     on a standard account add gp3, 200 GiB to match the Q&A reference.
   - Remove the Task group if one is shown. Do not enable cluster scaling.
4. **Cluster termination**: use **manual termination** (disable auto-termination), consistent with
   `config/emr/README.md`. If you instead enable an idle auto-termination timeout as a cost
   safeguard, record that as a separate configuration choice; it is not the same as manual
   termination. Terminate the cluster promptly at the end of each lab session.
5. **Software settings / Configurations**: choose "Enter configuration" and paste the contents of
   `config/emr/cluster_configurations_reference_m5xlarge.json`, replacing `REPLACE-WITH-YOUR-BUCKET-NAME` with your
   bucket. What each block does:
   - `hdfs-site dfs.replication=2` — the Q&A reference setting (every HDFS block on 2 nodes).
   - `spark-defaults` — 2 executors x 2 cores x 6 GiB, dynamic allocation off so the allocation is
     exactly what you report; `catalogImplementation=hive` so Spark can read Hive tables (brief 2.3).
   - `hive-site` — Hive on Tez (accepted per Q&A).
   - `jupyter-s3-conf` — saves notebooks to `s3://<bucket>/jupyter/` so they SURVIVE cluster
     termination. Without this, terminating the cluster deletes your notebook.
6. **Cluster logs**: enable, pointing to `s3://<bucket>/emr-logs/`. These logs are the job records
   the brief asks for ("unique IDs and traceable inputs and outputs").
7. **Security / EC2 key pair**: choose the lab key pair (AWS Academy: `vockey`; download
   `labsuser.pem` from the lab page). You need it to ssh.
8. **IAM roles**: AWS Academy: service role `EMR_DefaultRole`, instance profile `EMR_EC2_DefaultRole`
   (confirmed working 2026-10-28; `LabRole` is not valid as an instance profile). Standard account:
   the same two default roles, created automatically on first use. Never type access keys
   anywhere in the project.
9. Create cluster. Note the **Cluster ID (j-...)** and the creation time. It takes 10-15 minutes to
   reach the status **Waiting**. Record that time too.

Expected cost: about USD 0.9-1.0 per hour for the three nodes. Terminate when not working.

## C. Open the doors (security group)

By default nothing can connect to the cluster. On the cluster page -> Network and security ->
Primary node security group -> Edit inbound rules -> add:

- SSH, TCP 22, source "My IP"
- Custom TCP 9443, source "My IP" (JupyterHub)

## D. Connect and stage the data

From your laptop (Mac):

```
chmod 400 ~/Downloads/labsuser.pem
ssh -i ~/Downloads/labsuser.pem hadoop@<primary-node-public-DNS>
```

The prompt changes to `[hadoop@ip-... ~]$` — you are now typing on the cluster's primary node.

```
git clone https://github.com/hengouipui03/Big_Data_Project.git
cd Big_Data_Project
bash scripts/stage1_stage_to_hdfs.sh s3://<bucket>/raw /raw evidence/emr_ref_m5xlarge
```

The script: copies S3 -> HDFS with `hadoop distcp` (a MapReduce job, so it gets a YARN application
id), times it, lists HDFS, and reads every file back out of HDFS to recompute bytes, lines and sha256.

Expected results:

| Check | Expected |
|---|---|
| `/raw/prices/v2026-10-27/all_stocks_5yr.csv` bytes | 29,580,549 |
| its line count | 619,041 (619,040 rows + 1 header) |
| its sha256 | `6aea253cd19de60b568143991aaf1fa482456565c389205658d236e595e716cf` |
| each GDELT file sha256 | equals the value in `evidence/local/inventory.csv` |
| `hdfs_replication` | 2 |
| `yarn node -list` | 2 nodes RUNNING |

In the recorded 2026-10-28 run, every SHA-256 in
`evidence/emr_ref_m5xlarge/stage1_hdfs_inventory.csv` matched
`evidence/local/inventory.csv`. That is the Task 1 traceability chain:
source URL -> download (`acquisition_log.csv`) -> laptop (`inventory.csv`)
-> S3 -> HDFS (`stage1_hdfs_inventory.csv`).

Preserve the generated evidence before the cluster is terminated. For example, from the
primary node, copy it to S3:

```bash
aws s3 cp evidence/emr_ref_m5xlarge/ s3://<bucket>/evidence/emr_ref_m5xlarge/ --recursive
```

Then download or sync the evidence into your **local** Git clone, review the changed paths,
and commit/push from that clone. This avoids competing Git commits from the cluster and your
teammates' working copies. Ensure all submitted evidence is accurately described and traceable.

## E. Open JupyterHub and run one test cell

Browser: `https://<primary-node-public-DNS>:9443` (accept the certificate warning).
Default login: user `jovyan`, password `jupyter`.

New -> PySpark notebook. First cell:

```python
import socket, datetime
print(spark.version, socket.gethostname(), datetime.datetime.utcnow())
print(spark.sparkContext.applicationId)
spark.read.text("hdfs:///raw/prices/v2026-10-27/all_stocks_5yr.csv").count()
```

Expected: Spark 3.5.x, an application id `application_...`, and the count **619041**.
Save the notebook as `inf2006_pipeline.ipynb`. Because of `jupyter-s3-conf` it is also stored in
`s3://<bucket>/jupyter/jovyan/`.

## F. Finish the session

- Download or `git push` all evidence first (HDFS and the primary node's disk are lost on termination;
  S3 is not).
- EMR console -> Terminate cluster. Confirm the status becomes Terminated.
- Next session: create the cluster again with the same settings (clone from the terminated cluster:
  "Clone" button keeps every setting), then rerun section D (about 1-3 minutes for the copy).

## Re-running on a new cluster

1. Clone the terminated cluster in the EMR console (all settings kept) -> Create.
2. Re-add the port 9443 inbound rule; copy the new primary DNS.
3. `ssh`, then `sudo dnf install -y git`, `git clone ...` (or `git pull` in an existing clone — if git refuses
   because of untracked evidence files, remove the cluster's `evidence/emr_ref_m5xlarge` folder ONLY if
   that evidence is already pushed to GitHub).
4. Rerun section D (staging, ~1 min) and `scripts/stage2_streaming_and_hive.sh` (~3 min): the Hive
   metastore lives on the primary node and is lost with the cluster.
5. Run order matters: Hadoop Streaming and Hive first, then the Spark notebook. The notebook's Spark
   session holds the fixed 2 x (6+2) GiB executors and blocks MapReduce/Hive jobs; shut the kernel down
   (Kernel -> Shutdown) before running the scripts.

## Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| "VPC/subnet configuration was invalid: No route to any external sources" | The subnet is private — clone the cluster and pick a different (public) subnet in the default VPC |
| "insufficient EC2 permissions" / VALIDATION_ERROR (Academy) | Caused by the custom 200 GiB gp3 EBS volume on the core group — remove it and use default storage (m5.xlarge itself is allowed, as in Lab 7) |
| Cluster "terminated by user" after the lab session ended | Ending/expiring the Academy session terminates all clusters. Clone the cluster, re-add the security-group rules, rerun section D |
| Hadoop/Hive job stays at "Running job" with no progress | A Spark notebook session is holding the executors — `yarn application -list`, then `yarn application -kill <application_id of livy-session>` or Kernel -> Shutdown in JupyterHub |
| Notebook error "Session '0' not found" | The Spark session was killed — Kernel -> Restart in JupyterHub |
| "invalid instance profile LabRole" | LabRole is a service role, not an instance profile — use EMR_EC2_DefaultRole |
| `ssh: connection timed out` | Security group has no port-22 rule for your IP, or your IP changed — re-add "My IP" |
| `Permission denied (publickey)` | Wrong key file or wrong user — the user is `hadoop`, and the .pem must be chmod 400 |
| JupyterHub page does not load | Port 9443 rule missing; or use `https://` not `http://` |
| `distcp` fails with Access Denied on S3 | The cluster's EC2 role cannot read the bucket — use the lab's default role, check the bucket is in the same account/region |
| sha256 in HDFS differs from local | The upload to S3 is incomplete or a different file — re-upload and compare sizes |
| AWS Academy lab session ended | The cluster is terminated with the session; everything in S3 survives, HDFS does not |
