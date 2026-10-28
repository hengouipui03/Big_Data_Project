# EMR cluster configuration

`cluster_configurations_reference_m5xlarge.json` — paste into "Software settings" when creating the cluster
(replace REPLACE-WITH-YOUR-BUCKET-NAME; keep the placeholder in git).

Matches the brief v1.1 "provisional reference configuration" (Cloud resource constraints table):

| Setting | Reference | This file / cluster |
|---|---|---|
| EMR release | emr-7.10.0 | emr-7.10.0 |
| Cluster | 1 primary + 2 core m5.xlarge, no task nodes, no autoscaling | same ("Set cluster size manually") |
| Worker data storage | 200 GiB gp3 per core node, 3000 IOPS / 125 MiB/s | **Deviation:** AWS Academy rejected the custom volume (cluster j-06371601XD1JEGSGGUW6, 2026-10-28 03:15 UTC). EMR default storage used: 2 × 32 GiB gp2 per core node; HDFS capacity 115.75 GB physical ≈ 58 GB logical at replication 2 |
| HDFS replication | dfs.replication = 2 | hdfs-site dfs.replication=2 |
| Spark starting allocation | 2 executors × 2 cores × 6 GiB heap; dynamic allocation off | spark.executor.instances=2, cores=2, memory=6g, dynamicAllocation=false |
| Memory allowance | 2 GiB overhead per executor, 2 GiB driver heap | spark.executor.memoryOverhead=2g, spark.driver.memory=2g |
| Hive engine | Tez (Q&A) | hive-site hive.execution.engine=tez |
| Notebook | JupyterHub (Q&A) | jupyter-s3-conf persists notebooks to s3://<bucket>/jupyter/ |

Other settings: applications Hadoop, Hive, Tez, Spark, Livy, JupyterHub; logs to s3://<bucket>/emr-logs/;
termination = manual; AWS Academy roles EMR_DefaultRole / EMR_EC2_DefaultRole; key pair vockey.

The brief (v1.1) requires the "justified alternative" to be a **software** change (executor allocation,
partitions, joins, intermediate storage) with hardware, release, replication and storage performance fixed.
The alternative configuration will be added here as a second JSON file once chosen (Stage 6).
