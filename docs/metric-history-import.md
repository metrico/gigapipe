# Metric history import

When a deployment that already holds metric data first starts with the metric tables, it copies
the metric history of `samples_v3`, `time_series` and `metrics_15s` into them in the background.
[Configuration](configuration.md#upgrading-a-deployment-with-metric-history) lists what an
operator needs; this page describes how the copy runs.

## What is copied

- **Series and metadata.** The series rows and metric metadata of every metric series in
  `time_series`, first and again at the end.
- **Raw history.** The metric rows of `samples_v3` up to the hour before the metric tables were
  created, one day at a time, newest day first, back to the day of the oldest row and no further
  than `SAMPLES_DAYS`. Each sample is copied with the previous sample of its series, so the
  aggregate tiers count resets and rates across day boundaries.
- **15-second history.** The `metrics_15s` rows older than the raw history, one week at a time,
  each row becoming one sample carrying its last value.
- **Tail.** The metric rows that instances of the earlier release keep adding to `samples_v3`,
  an hour of rows at a time, until none has arrived for an hour.

The import then records itself complete in the `settings` table and does not run again. A
deployment without metric rows completes at once.

## One instance at a time

- The instance running the import holds a lease, a record in the `settings` table naming it and
  the time it last wrote it.
- The holder renews the lease every minute. Another instance takes it over once it is three
  minutes old by the ClickHouse server's clock.
- An instance reads the lease back a few seconds after taking it and runs only if it still holds
  it. It checks again before each day or week.
- Instances that do not hold the lease retry every minute.

## Progress and restarts

- Each day of raw history and each week of 15-second history is recorded started before its
  copy and done after it. A restart skips the units recorded done.
- A unit recorded started but not done was interrupted. Its query, run under a query ID derived
  from the database and the unit, is killed, its buckets are deleted from the aggregate tiers,
  and it is copied again.
- That deletion also drops the tier contribution of samples the new release accepted with
  timestamps in that unit, at or before the hour before the upgrade: a replayed backlog or a
  lagging sender. Raw keeps them.
- The tail's progress is a watermark record, so a restart resumes the tail where it stopped.

## On a cluster

- With `CLUSTER_NAME` set, the import runs on the node the instance connects to, reading and
  writing the `_dist` tables. Each copy uses `insert_distributed_sync = 1`, so a unit recorded
  done is on its shards.
- The lease and progress records are written through `settings_dist` and read as the latest
  value per record, never with `FINAL`.
- A unit copied again is first deleted from the aggregate tiers on every node, and the
  interrupted query is killed on every node (`ON CLUSTER`).
- The creation time of the metric tables is recorded on the node that ran the migration. On a
  cluster without `Cloud` and with several replicas per shard, the import retries until a read
  through `settings_dist` reaches the replica holding it.
