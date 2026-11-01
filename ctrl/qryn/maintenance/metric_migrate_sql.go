package maintenance

import (
	"fmt"
	"time"
)

// Statement ops, naming each step for logs and tests.
const (
	opRecords   = "records"
	opRecord    = "record"
	opRecordAge = "record_age"
	opExists    = "exists"
	opCount     = "count"
	opCopy      = "copy"
	opDelete    = "delete"
	opRebuild   = "rebuild"
	opSeries    = "series"
	opMetadata  = "metadata"
)

const (
	legacySamples    = "samples_v3"
	legacySeries     = "time_series"
	legacyRollup     = "metrics_15s"
	rawSamples       = "metric_samples"
	seriesTable      = "metric_series"
	metadataTable    = "metric_metadata"
	legacyMetricRows = "type IN (2, 0)"
	msNs             = int64(time.Millisecond)
)

const tierColumns = "fingerprint, bucket, first, last, count, sum, var, min, max, resets, reset_drop, changes, stale_at"

// msBounds selects the timestamps in (from, to].
func msBounds(column string, from, to time.Time) string {
	return fmt.Sprintf("%s > fromUnixTimestamp64Milli(%d) AND %s <= fromUnixTimestamp64Milli(%d)",
		column, from.UnixMilli(), column, to.UnixMilli())
}

// countRawSQL counts the raw samples of the unit's hour.
func countRawSQL(u migrationUnit) string {
	return fmt.Sprintf("SELECT count() FROM %s FINAL WHERE %s", rawSamples, msBounds("timestamp", u.from, u.to))
}

// copyRawSQL copies the samples_v3 metric rows whose ms lies in the unit's hour into raw.
func copyRawSQL(u migrationUnit) string {
	return fmt.Sprintf("INSERT INTO %s (fingerprint, timestamp, value) "+
		"SELECT fingerprint, fromUnixTimestamp64Milli(intDiv(timestamp_ns, 1000000)) AS timestamp, value "+
		"FROM %s WHERE %s AND timestamp_ns >= %d AND timestamp_ns < %d",
		rawSamples, legacySamples, legacyMetricRows, (u.from.UnixMilli()+1)*msNs, (u.to.UnixMilli()+1)*msNs)
}

// deleteTierSQL deletes the tier's buckets of the unit's hour.
func deleteTierSQL(tier metricTier, u migrationUnit) string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s", tier.Table, msBounds("bucket", u.from, u.to))
}

// rebuildTierSQL aggregates the raw samples of the unit's hour into the tier. Each sample's
// predecessor is the latest earlier non-stale sample of its series within the hour.
func rebuildTierSQL(tier metricTier, u migrationUnit) string {
	input := fmt.Sprintf("(SELECT fingerprint, timestamp, value, "+
		"prev.1 AS prev_timestamp, prev.2 AS prev_value, 1 AS aggregate "+
		"FROM (SELECT fingerprint, timestamp, value, "+
		"argMaxIf((timestamp, value), timestamp, reinterpretAsUInt64(value) != 0x7ff0000000000002) "+
		"OVER (PARTITION BY fingerprint ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev "+
		"FROM %s FINAL WHERE %s))", rawSamples, msBounds("timestamp", u.from, u.to))
	return fmt.Sprintf("INSERT INTO %s (%s) %s", tier.Table, tierColumns, tierAggregation(tier, input))
}

// existsSQL reads whether a table exists.
func existsSQL(table string) string {
	return "EXISTS TABLE " + table
}

// seriesSQL copies each metric series seen on or after since's day, or every one when since
// is zero. With rollup set, first_seen reaches back to the series' oldest metrics_15s row.
func seriesSQL(rollup bool, since time.Time) string {
	firstSeen := "toDateTime64(first_day, 3, 'UTC')"
	join := ""
	if rollup {
		firstSeen = fmt.Sprintf("if(rollup_first_ns > 0, least(%s, "+
			"fromUnixTimestamp64Milli(intDiv(rollup_first_ns, 1000000), 'UTC')), %s)", firstSeen, firstSeen)
		join = fmt.Sprintf(" LEFT JOIN (SELECT fingerprint, min(timestamp_ns) AS rollup_first_ns FROM %s "+
			"WHERE %s GROUP BY fingerprint) AS rollup USING fingerprint", legacyRollup, legacyMetricRows)
	}
	where := legacyMetricRows
	if !since.IsZero() {
		where += fmt.Sprintf(" AND date >= toDate('%s')", since.UTC().Format(time.DateOnly))
	}
	return fmt.Sprintf("INSERT INTO %s (name, fingerprint, labels, first_seen, last_seen) "+
		"SELECT if(series_name != '', series_name, JSONExtractString(series_labels, '__name__')) AS name, fingerprint, "+
		"CAST(JSONExtractKeysAndValues(series_labels, 'String') AS Map(LowCardinality(String), String)) AS labels, "+
		"%s AS first_seen, "+
		"toDateTime64(last_day + INTERVAL 1 DAY, 3, 'UTC') AS last_seen "+
		"FROM (SELECT fingerprint, any(name) AS series_name, any(labels) AS series_labels, "+
		"min(date) AS first_day, max(date) AS last_day "+
		"FROM %s WHERE %s GROUP BY fingerprint) AS series%s", seriesTable, firstSeen, legacySeries, where, join)
}

// metadataSQL copies the latest metadata of each metric family.
func metadataSQL() string {
	return fmt.Sprintf("INSERT INTO %s (name, type, help, unit, updated_at) "+
		"SELECT name, JSONExtractString(latest, 'type') AS type, JSONExtractString(latest, 'help') AS help, "+
		"JSONExtractString(latest, 'unit') AS unit, fromUnixTimestamp64Milli(intDiv(latest_ns, 1000000)) AS updated_at "+
		"FROM (SELECT name, argMax(metadata, updated_at_ns) AS latest, max(updated_at_ns) AS latest_ns "+
		"FROM %s WHERE %s AND metadata != '' GROUP BY name)", metadataTable, legacySeries, legacyMetricRows)
}

// unitQueryID is the query id each statement of a unit runs under.
func unitQueryID(db string, u migrationUnit) string {
	return fmt.Sprintf("metric_migrate-%s-%s-%d", db, u.kind, u.to.UnixMilli())
}
