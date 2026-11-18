# Nine-run benchmark draft

This is a draft that has not been exercised on EMR. Do not use its results as
assessed evidence until a pilot confirms the Spark runner, Hive paths, resource
collection and all required output checks on the actual cluster.

The launcher creates isolated Hive databases `bench_run01` through `bench_run09`,
with separate Streaming outputs, prepared datasets and models. A Spark submission
executes the calculation cells from the project under each matching database; it
does not execute the notebook's old partial-benchmark repetitions or plotting
cells. It does not implement prediction weights: obtain the required formula and
include that output and its checks in the timed workload before assessed runs.

The launcher requires two live workers, HDFS replication 2, an empty destination,
the full project files, S3 access and passwordless batch SSH from the primary to
both workers. It records worker `df` samples approximately every 10 seconds. If
SSH is unavailable, preflight stops before staging. Forwarding an SSH agent is
one possible setup; never put a private key into S3. Resource observations are
sampled, and missed or failed samples must be reported. Confirm that Spark UI
executor peak metrics are enabled and accessible from the Spark driver.

Staging and input checksums are outside the timer. Each timer spans the Streaming
and Hive script followed by a new Spark application that prepares, fits models,
computes analytical tables and runs checks. Record any difference from the
teaching team's confirmed timing policy. Plots and a fresh-session reload are not
part of this timed workload. The script preserves those run's Hive table files,
prepared files, Spark evidence and model files. It stops after a failure and
refuses to reuse an existing destination. Recovery/resume has not been tested.
After a failure before Hive database creation, `BENCH_ATTEMPT=attempt-02` creates
an independent subfolder and `BENCH_REUSE_INPUTS=1` reuses the initially staged
inputs while rechecking every SHA-256. It does not resume a partial run and it
still refuses to overwrite existing Hive databases or outputs. If any run already
created a `bench_runNN` database, stop and preserve it; do not drop it merely to
make a retry work.

The comparison viewer is `notebooks/benchmark_evidence_viewer.ipynb`: upload it
through JupyterHub under jovyan and select the required run. It reads saved Hive
table files from S3, so the original cluster need not still exist. This does not
create Hive metastore tables on a new cluster.

The 7-day profile is December 2015; the 91-day candidate profile is 2016 Q1.
Both use the same price CSV. The seven-day news coverage cannot support the full
news analysis; the existing guard records that limitation. Confirm with the
teaching team that the selected profiles and alternative are permitted.
