package maintenance

import (
	"fmt"
	"time"
)

// importTables names the tables the metric import reads and writes.
type importTables struct {
	timeSeries, samples, rollup string
	staging, series, metadata   string
	tiers                       []string
}

var localImportTables = importTables{
	timeSeries: "time_series",
	samples:    "samples_v3",
	rollup:     "metrics_15s",
	staging:    "metric_samples_in",
	series:     "metric_series",
	metadata:   "metric_metadata",
	tiers:      []string{"metrics_5m", "metrics_1h"},
}

const (
	metricRows = "type IN (2, 0)"
	msNs       = int64(time.Millisecond)
)

// stagedRows inserts rows (fingerprint, timestamp_ns, timestamp, value) into the staging table,
// each with the previous non-stale sample of its series as its predecessor, filtered by where.
func stagedRows(t importTables, rows, where string) string {
	return fmt.Sprintf("INSERT INTO %s (fingerprint, timestamp, value, prev_timestamp, prev_value, aggregate) "+
		"SELECT fingerprint, timestamp, value, "+
		"prev.1 AS prev_timestamp, prev.2 AS prev_value, prev_timestamp < timestamp AS aggregate "+
		"FROM (SELECT fingerprint, timestamp_ns, timestamp, value, "+
		"maxIf((timestamp, value), reinterpretAsUInt64(value) != 0x7ff0000000000002) "+
		"OVER (PARTITION BY fingerprint ORDER BY timestamp_ns ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev "+
		"FROM (%s)) "+
		"WHERE %s", t.staging, rows, where)
}

func msBounds(from, to time.Time) string {
	return fmt.Sprintf("timestamp > fromUnixTimestamp64Milli(%d) AND timestamp <= fromUnixTimestamp64Milli(%d)",
		from.UnixMilli(), to.UnixMilli())
}

// chunkSQL copies the raw metric rows of (from, to], floored to ms, with predecessors from a day earlier.
func chunkSQL(t importTables, from, to time.Time) string {
	lo := (from.Add(-day).UnixMilli() + 1) * msNs
	hi := (to.UnixMilli() + 1) * msNs
	rows := fmt.Sprintf("SELECT fingerprint, timestamp_ns, "+
		"fromUnixTimestamp64Milli(intDiv(timestamp_ns, 1000000)) AS timestamp, value "+
		"FROM %s WHERE %s AND timestamp_ns >= %d AND timestamp_ns < %d", t.samples, metricRows, lo, hi)
	return stagedRows(t, rows, msBounds(from, to))
}

// spanSQL turns each rollup row of (from, to] lying wholly below floor into one sample at its
// start carrying the row's last value, with predecessors from a day earlier.
func spanSQL(t importTables, from, to, floor time.Time) string {
	rows := fmt.Sprintf("SELECT fingerprint, timestamp_ns, "+
		"fromUnixTimestamp64Milli(intDiv(timestamp_ns, 1000000)) AS timestamp, argMaxMerge(last) AS value "+
		"FROM %s WHERE %s AND timestamp_ns > %d AND timestamp_ns <= %d AND timestamp_ns < %d "+
		"GROUP BY fingerprint, timestamp_ns",
		t.rollup, metricRows, from.Add(-day).UnixNano(), to.UnixNano(), floor.UnixNano())
	return stagedRows(t, rows, msBounds(from, to))
}

// tailSQL copies the raw metric rows of (watermark, upper] in ns, with predecessors from an hour earlier.
func tailSQL(t importTables, watermark, upper int64) string {
	rows := fmt.Sprintf("SELECT fingerprint, timestamp_ns, "+
		"fromUnixTimestamp64Milli(intDiv(timestamp_ns, 1000000)) AS timestamp, value "+
		"FROM %s WHERE %s AND timestamp_ns > %d AND timestamp_ns <= %d",
		t.samples, metricRows, watermark-int64(time.Hour), upper)
	return stagedRows(t, rows, fmt.Sprintf("timestamp_ns > %d", watermark))
}

// redoSQL deletes the tier buckets of a unit's (from, to].
func redoSQL(t importTables, u importUnit) []string {
	var out []string
	for _, tier := range t.tiers {
		out = append(out, fmt.Sprintf("DELETE FROM %s WHERE bucket > fromUnixTimestamp64Milli(%d) "+
			"AND bucket <= fromUnixTimestamp64Milli(%d)", tier, u.from.UnixMilli(), u.to.UnixMilli()))
	}
	return out
}

// seriesSQL copies each metric series, seen from its first day to the end of its last.
func seriesSQL(t importTables) string {
	return fmt.Sprintf("INSERT INTO %s (name, fingerprint, labels, first_seen, last_seen) "+
		"SELECT if(series_name != '', series_name, JSONExtractString(series_labels, '__name__')) AS name, fingerprint, "+
		"CAST(JSONExtractKeysAndValues(series_labels, 'String') AS Map(LowCardinality(String), String)) AS labels, "+
		"toDateTime64(first_day, 3, 'UTC') AS first_seen, "+
		"toDateTime64(last_day + INTERVAL 1 DAY, 3, 'UTC') AS last_seen "+
		"FROM (SELECT fingerprint, any(name) AS series_name, any(labels) AS series_labels, "+
		"min(date) AS first_day, max(date) AS last_day "+
		"FROM %s WHERE %s GROUP BY fingerprint)", t.series, t.timeSeries, metricRows)
}

// metadataSQL copies the latest metadata of each metric family.
func metadataSQL(t importTables) string {
	return fmt.Sprintf("INSERT INTO %s (name, type, help, unit, updated_at) "+
		"SELECT name, JSONExtractString(latest, 'type') AS type, JSONExtractString(latest, 'help') AS help, "+
		"JSONExtractString(latest, 'unit') AS unit, fromUnixTimestamp64Milli(intDiv(latest_ns, 1000000)) AS updated_at "+
		"FROM (SELECT name, argMax(metadata, updated_at_ns) AS latest, max(updated_at_ns) AS latest_ns "+
		"FROM %s WHERE %s AND metadata != '' GROUP BY name)", t.metadata, t.timeSeries, metricRows)
}
