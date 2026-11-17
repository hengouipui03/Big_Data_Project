# EMR reference configuration

Use EMR 7.10.0 with one primary and two core m5.xlarge nodes, no task nodes or autoscaling. Enable Hadoop, Hive/Tez, Spark, Livy and JupyterHub. Set HDFS replication to 2. The supplied configuration JSON requests two Spark executors, two cores and 6g heap each, 2g overhead, 2g driver, dynamic allocation disabled and Hive catalog integration. Replace its Jupyter persistence bucket before creating a cluster.

For storage, the teaching reference is a 30 GiB root volume per node and one 200 GiB gp3 data volume per core (3,000 IOPS, 125 MiB/s). Earlier team clusters used two 100 GiB data volumes per core; identical total capacity does not imply identical provisioned throughput. Final-run instance-group exports were not present in the handover, so the earlier disk arrangement is not claimed as independently verified for the final cluster.

Final cluster identity, replication, staging duration, worker nodes and capacity are recorded in `evidence/final_run/stage1_cluster.txt` and `stage1_dfs_report.txt`. The notebook records actual Spark allocation and measured executor memory. Follow `docs/RUN_ON_EMR.md`; this JSON is a reference configuration, not an exported final-cluster description.
