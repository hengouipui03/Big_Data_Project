# EMR cluster profiles

| File | Nodes | Use | Account |
|---|---|---|---|
| `cluster_configurations_reference_m5xlarge.json` | 1 primary + 2 core **m5.xlarge**, EMR default storage (Academy rejects a custom 200 GiB gp3 volume; add it on a standard account) | Assessed runs (brief: "Cloud resource constraints"; Q&A reference config) | AWS Academy Learner Lab (works, as in Lab 7) |
| `cluster_configurations_dev_m5large.json` | 1 primary + 2 core **m5.large**, EMR default storage | "Justified alternative" comparison (Q&A Tip 1) | AWS Academy Learner Lab |

Both set HDFS replication 2, Hive on Tez, Spark catalog = Hive, notebook persistence to S3.
Spark allocation differs because an m5.large node has 8 GiB RAM (YARN ~6 GiB usable):
reference = 2 executors x 2 cores x 6 GiB; dev = 2 executors x 1 core x 3 GiB.

Common settings: release emr-7.10.0; applications Hadoop, Hive, Tez, Spark, Livy, JupyterHub;
no task nodes; cluster size set manually (no autoscaling); logs to s3://<bucket>/emr-logs/.
AWS Academy roles: service role EMR_DefaultRole, instance profile EMR_EC2_DefaultRole, key pair vockey.
Replace REPLACE-WITH-YOUR-BUCKET-NAME when pasting into the console; keep the placeholder in git.
