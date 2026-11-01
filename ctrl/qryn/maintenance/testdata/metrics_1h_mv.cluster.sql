CREATE MATERIALIZED VIEW IF NOT EXISTS cloki.metrics_1h_mv ON CLUSTER `c1` TO cloki.metrics_1h AS
WITH
  reinterpretAsUInt64(value) = 0x7ff0000000000002 AS stale,
  reinterpretAsUInt64(prev_value) = 0x7ff0000000000002 AS prev_stale,
  toStartOfInterval(timestamp - INTERVAL 1 MILLISECOND, INTERVAL 1 HOUR) + INTERVAL 1 HOUR AS bucket_end,
  prev_timestamp < timestamp
    AND toStartOfInterval(prev_timestamp - INTERVAL 1 MILLISECOND, INTERVAL 1 HOUR) + INTERVAL 1 HOUR = bucket_end
    AND NOT stale AND NOT prev_stale AS paired
SELECT
  fingerprint,
  toDateTime64(bucket_end, 3) AS bucket,
  minIfState((timestamp, value), NOT stale) AS first,
  maxIfState((timestamp, value), NOT stale) AS last,
  countIf(NOT stale) AS count,
  sumIf(value, NOT stale) AS sum,
  varPopStableIfState(value, NOT stale) AS var,
  minIfState(if(isNaN(value), inf, value), NOT stale) AS min,
  maxIfState(if(isNaN(value), -inf, value), NOT stale) AS max,
  countIf(paired AND value < prev_value) AS resets,
  sumIf(prev_value, paired AND value < prev_value) AS reset_drop,
  countIf(paired AND value != prev_value AND NOT (isNaN(value) AND isNaN(prev_value))) AS changes,
  maxIf(timestamp, stale) AS stale_at
FROM cloki.metric_samples_in
WHERE aggregate
GROUP BY fingerprint, bucket