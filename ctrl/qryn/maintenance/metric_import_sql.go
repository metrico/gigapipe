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
	// settings holds the import's records and the metric stack's migration time.
	settings string
	// onCluster runs deletes and kills on every node.
	onCluster string
	// insertSettings ends every insert.
	insertSettings string
}

var localImportTables = importTables{
	timeSeries: "time_series",
	samples:    "samples_v3",
	rollup:     "metrics_15s",
	staging:    "metric_samples_in",
	series:     "metric_series",
	metadata:   "metric_metadata",
	tiers:      []string{"metrics_5m", "metrics_1h"},
	settings:   "settings",
}

// clusterImportTables reads and writes through the distributed tables of cluster. Each insert
// returns once its rows are on their shards.
func clusterImportTables(cluster string) importTables {
	return importTables{
		timeSeries:     "time_series_dist",
		samples:        "samples_v3_dist",
		rollup:         "metrics_15s_dist",
		staging:        "metric_samples_in_dist",
		series:         "metric_series_dist",
		metadata:       "metric_metadata_dist",
		tiers:          []string{"metrics_5m", "metrics_1h"},
		settings:       "settings_dist",
		onCluster:      " ON CLUSTER `" + cluster + "`",
		insertSettings: " SETTINGS insert_distributed_sync = 1",
	}
}

const (
	metricRows = "type IN (2, 0)"
	msNs       = int64(time.Millisecond)
)

// stagedRows inserts rows (fingerprint, timestamp_ns, timestamp, value) into the staging table,
// filtered by where. Each row's predecessor is the latest earlier non-stale sample of its series,
// the first row by ns within its ms.
func stagedRows(t importTables, rows, where string) string {
	return fmt.Sprintf("INSERT INTO %s (fingerprint, timestamp, value, prev_timestamp, prev_value, aggregate) "+
		"SELECT fingerprint, timestamp, value, "+
		"prev.1 AS prev_timestamp, prev.2 AS prev_value, prev_timestamp < timestamp AS aggregate "+
		"FROM (SELECT fingerprint, timestamp_ns, timestamp, value, "+
		"argMaxIf((timestamp, value), (timestamp, -timestamp_ns), reinterpretAsUInt64(value) != 0x7ff0000000000002) "+
		"OVER (PARTITION BY fingerprint ORDER BY timestamp_ns ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev "+
		"FROM (%s)) "+
		"WHERE %s%s", t.staging, rows, where, t.insertSettings)
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
		out = append(out, fmt.Sprintf("DELETE FROM %s%s WHERE bucket > fromUnixTimestamp64Milli(%d) "+
			"AND bucket <= fromUnixTimestamp64Milli(%d)", tier, t.onCluster, u.from.UnixMilli(), u.to.UnixMilli()))
	}
	return out
}

// seriesSQL copies each metric series, seen from its first day, or its oldest rollup row when
// rollup is set and that is earlier, to the end of its last day.
func seriesSQL(t importTables, rollup bool) string {
	firstSeen := "toDateTime64(first_day, 3, 'UTC')"
	join := ""
	if rollup {
		firstSeen = fmt.Sprintf("if(rollup_first_ns > 0, least(%s, "+
			"fromUnixTimestamp64Milli(intDiv(rollup_first_ns, 1000000), 'UTC')), %s)", firstSeen, firstSeen)
		join = fmt.Sprintf(" LEFT JOIN (SELECT fingerprint, min(timestamp_ns) AS rollup_first_ns FROM %s "+
			"WHERE %s GROUP BY fingerprint) AS rollup USING fingerprint", t.rollup, metricRows)
	}
	return fmt.Sprintf("INSERT INTO %s (name, fingerprint, labels, first_seen, last_seen) "+
		"SELECT if(series_name != '', series_name, JSONExtractString(series_labels, '__name__')) AS name, fingerprint, "+
		"CAST(JSONExtractKeysAndValues(series_labels, 'String') AS Map(LowCardinality(String), String)) AS labels, "+
		"%s AS first_seen, "+
		"toDateTime64(last_day + INTERVAL 1 DAY, 3, 'UTC') AS last_seen "+
		"FROM (SELECT fingerprint, any(name) AS series_name, any(labels) AS series_labels, "+
		"min(date) AS first_day, max(date) AS last_day "+
		"FROM %s WHERE %s GROUP BY fingerprint) AS series%s%s",
		t.series, firstSeen, t.timeSeries, metricRows, join, t.insertSettings)
}

// metadataSQL copies the latest metadata of each metric family.
func metadataSQL(t importTables) string {
	return fmt.Sprintf("INSERT INTO %s (name, type, help, unit, updated_at) "+
		"SELECT name, JSONExtractString(latest, 'type') AS type, JSONExtractString(latest, 'help') AS help, "+
		"JSONExtractString(latest, 'unit') AS unit, fromUnixTimestamp64Milli(intDiv(latest_ns, 1000000)) AS updated_at "+
		"FROM (SELECT name, argMax(metadata, updated_at_ns) AS latest, max(updated_at_ns) AS latest_ns "+
		"FROM %s WHERE %s AND metadata != '' GROUP BY name)%s", t.metadata, t.timeSeries, metricRows, t.insertSettings)
}

// unitQueryID is the query id a unit's insert runs under, so one insert per unit runs at a time.
func unitQueryID(db, name string) string {
	return "metric_import-" + db + "-" + name
}

// killSQL stops a query by id, with every query it started, and waits for them to end.
func killSQL(t importTables, queryID string) string {
	return fmt.Sprintf("KILL QUERY%s WHERE initial_query_id = '%s' SYNC", t.onCluster, queryID)
}

// recordsSQL reads the latest name and value of each import record of the type $1.
func recordsSQL(t importTables) string {
	return fmt.Sprintf("SELECT argMax(name, inserted_at), argMax(value, inserted_at) "+
		"FROM %s WHERE type = $1 GROUP BY fingerprint", t.settings)
}

// recordPutSQL writes a record: fingerprint $1, type $2, name $3, value $4.
func recordPutSQL(t importTables) string {
	return fmt.Sprintf("INSERT INTO %s (fingerprint, type, name, value, inserted_at) "+
		"SELECT $1, $2, $3, $4, now64(9)%s", t.settings, t.insertSettings)
}

// recordAgeSQL is the server time in ms since the record of fingerprint $1 was last written.
func recordAgeSQL(t importTables) string {
	return fmt.Sprintf("SELECT dateDiff('millisecond', max(inserted_at), now64(9)) FROM %s WHERE fingerprint = $1",
		t.settings)
}

// migrationTimeSQL reads T0, the unix time the metric stack was created.
func migrationTimeSQL(t importTables) string {
	return fmt.Sprintf("SELECT argMax(value, inserted_at) FROM %s WHERE type = 'update' AND name = 'metric_stack'",
		t.settings)
}
