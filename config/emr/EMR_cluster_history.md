
# EMR Cluster History

This document records the two principal EMR clusters used
for the INF2006 Big Data project.

## Cluster comparison

| Setting | 4 November 2026 | 13 November 2026 |
|---|---|---|
| Cluster ID | j-06878942C5H7E0OZ24GA | j-0116410E858ST6KQS49 |
| EMR release | emr-7.10.0 | emr-7.10.0 |
| Primary nodes | 1 x m5.xlarge | 1 x m5.xlarge |
| Core nodes | 2 x m5.xlarge | 2 x m5.xlarge |
| Hadoop | 3.4.1 | 3.4.1 |
| Hive | 3.1.3 | 3.1.3 |
| Spark | 3.5.5 | 3.5.5 |
| Livy | 0.8.0 | 0.8.0 |

## EBS configuration

### Primary instance group
- Two additional 32 GiB gp2 EBS volumes.

### Core instance group
- Two additional 100 GiB gp3 EBS volumes per node.
- Total additional gp3 storage: 200 GiB per core node.
- IOPS: 3,000 per volume.
- Throughput: 125 MiB/s per volume.

### Root volume
The root volume size is not recorded in the
exported cluster metadata and has not been independently
verified.

## S3 emr-logs
put into s3://inf2006-team07-bigdata-1-310947418945-us-east-1-an/emr-logs/

## Spark configuration

- Driver memory: 2 GB
- Executor instances: 2
- Executor cores: 2
- Executor memory: 6 GB
- Executor memory overhead: 2 GB
- Dynamic allocation: disabled
- HDFS replication: 2

## Bootstrap actions

Neither cluster had configured EMR bootstrap actions.

## Historical metadata

The full AWS CLI exports are retained separately:

- nov04-cluster.json
- nov04-instance-groups.json
- nov04-instances.json
- nov04-steps.json
- nov13-cluster.json
- nov13-instance-groups.json
- nov13-instances.json
- nov13-steps.json
