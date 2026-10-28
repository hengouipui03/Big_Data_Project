# Stage 1 — EMR cluster and raw corpus in HDFS

Goal: a running EMR cluster with the raw files in HDFS and dated evidence that proves it.
This document is the first draft of **Appendix A** (EMR setup and rerun guide).

Reference configuration (from the course Q&A): emr-7.10.0; 1 primary + 2 core m5.xlarge; no task
nodes, no autoscaling; one 200 GiB gp3 volume (3000 IOPS, 125 MiB/s) per core node; HDFS
replication 2; Spark 2 executors x 2 cores x 6 GiB.

**AWS Academy finding (2026-10-28):** the Learner Lab account launches m5.xlarge EMR nodes (as in
Lab 7) but rejected the cluster when a custom 200 GiB gp3 volume (3000 IOPS, 125 MiB/s) was added to
the core group ("insufficient EC2 permissions" / VALIDATION_ERROR). The assessed cluster therefore
uses EMR's default instance storage. gp3's baseline performance is already 3000 IOPS / 125 MiB/s, so
only capacity differs from the reference; the storage estimate in report section 6 shows the chosen
corpus fits. (A dev profile on m5.large, `config/emr/cluster_configurations_dev_m5large.json`, is
kept as the "justified alternative" for the Q&A Tip 1 comparison.)
Evidence folders: `evidence/emr_ref_m5xlarge/` (assessed profile) and `evidence/emr_dev_m5large/`.

Fill in the blanks below as you go — they become the values in Appendix A.

| Item | Value |
|---|---|
| Profile (reference m5.xlarge / dev m5.large) | |
| AWS account | AWS Academy Learner Lab |
| Core-node storage | EMR default (custom 200 GiB gp3 volume rejected by Academy) |
| AWS region | |
| S3 bucket name | |
| Cluster ID (j-...) | |
| Cluster created (UTC) | |
| Cluster "Waiting" at (UTC) | |
| Primary node public DNS | |
| Key pair name | |
| Staging seconds (from stage1_cluster.txt) | |
| distcp application id | |

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

   Alternative with the AWS CLI (needs the lab's temporary credentials in `~/.aws/credentials`):
   `aws s3 sync data/raw s3://<bucket>/raw/ --exclude "*" --include "prices/*" --include "gdelt/*"`
   (then rename into the `v<date>` folders, or upload with the paths above).

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
4. **Cluster termination**: turn OFF auto-termination for now (you control it) but set an idle
   timeout of 1 hour as a safety net.
5. **Software settings / Configurations**: choose "Enter configuration" and paste the contents of
   the matching profile file in `config/emr/` (reference or dev), replacing `REPLACE-WITH-YOUR-BUCKET-NAME` with your
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
bash scripts/stage1_stage_to_hdfs.sh s3://<bucket>/raw /raw evidence/emr_dev_m5large   # or evidence/emr_ref_m5xlarge
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

If every sha256 in `evidence/emr/stage1_hdfs_inventory.csv` matches `evidence/local/inventory.csv`,
the corpus in HDFS is provably the file the brief links to. That is the Task 1 traceability chain:
brief link -> download (acquisition_log) -> laptop (inventory) -> S3 -> HDFS (stage1_hdfs_inventory).

Save the evidence back to GitHub from the primary node:

```
git add evidence/emr
git -c user.name="<name>" -c user.email="<email>" commit -m "Stage 1: EMR staging evidence <cluster id>"
git push        # needs a GitHub token; alternatively copy the files to S3:
aws s3 cp evidence/emr s3://<bucket>/evidence/emr/ --recursive
```

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

## Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| "VPC/subnet configuration was invalid: No route to any external sources" | The subnet is private — clone the cluster and pick a different (public) subnet in the default VPC |
| "insufficient EC2 permissions" / VALIDATION_ERROR (Academy) | Caused by the custom 200 GiB gp3 EBS volume on the core group — remove it and use default storage (m5.xlarge itself is allowed, as in Lab 7) |
| Cluster "terminated by user" after the lab session ended | Ending/expiring the Academy session terminates all clusters. Clone the cluster, re-add the security-group rules, rerun section D |
| "invalid instance profile LabRole" | LabRole is a service role, not an instance profile — use EMR_EC2_DefaultRole |
| `ssh: connection timed out` | Security group has no port-22 rule for your IP, or your IP changed — re-add "My IP" |
| `Permission denied (publickey)` | Wrong key file or wrong user — the user is `hadoop`, and the .pem must be chmod 400 |
| JupyterHub page does not load | Port 9443 rule missing; or use `https://` not `http://` |
| `distcp` fails with Access Denied on S3 | The cluster's EC2 role cannot read the bucket — use the lab's default role, check the bucket is in the same account/region |
| sha256 in HDFS differs from local | The upload to S3 is incomplete or a different file — re-upload and compare sizes |
| AWS Academy lab session ended | The cluster is terminated with the session; everything in S3 survives, HDFS does not |
