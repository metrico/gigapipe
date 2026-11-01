package maintenance

import (
	"fmt"
	"math/bits"
	"strings"
	"time"
)

// Statement ops, naming each step for logs and tests.
const (
	opRecords   = "records"
	opRecord    = "record"
	opRecordAge = "record_age"
	opExists    = "exists"
	op15sHours  = "15s_hours"
	opCount     = "count"
	opCheck     = "check"
	opCopy      = "copy"
	opDelete    = "delete"
	opRebuild   = "rebuild"
	opSeries    = "series"
	opMetadata  = "metadata"
	opKill      = "kill"
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

// nonStale holds for a sample whose value is not the stale marker.
const nonStale = "reinterpretAsUInt64(value) != 0x7ff0000000000002"

// migrationTables names the tables a run reads and writes: the local tables on a single node,
// their _dist tables on a cluster. Tier deletes always name the local tier.
type migrationTables struct {
	cluster                                              string
	samples, series, rollup, raw, seriesOut, metadataOut string
	settings                                             string
}

// tablesFor names the tables of a run on cluster, or on a single node when cluster is empty.
func tablesFor(cluster string) migrationTables {
	t := migrationTables{
		cluster:     cluster,
		samples:     legacySamples,
		series:      legacySeries,
		rollup:      legacyRollup,
		raw:         rawSamples,
		seriesOut:   seriesTable,
		metadataOut: metadataTable,
		settings:    "settings",
	}
	if cluster != "" {
		for _, n := range []*string{&t.samples, &t.series, &t.rollup, &t.raw, &t.seriesOut, &t.metadataOut, &t.settings} {
			*n += distSuffix
		}
	}
	return t
}

const distSuffix = "_dist"

// tier names the table the tier's counts and rebuilds go through.
func (t migrationTables) tier(tier metricTier) string {
	if t.cluster != "" {
		return tier.Table + distSuffix
	}
	return tier.Table
}

// onCluster is the ON CLUSTER clause of a run on a cluster, with a leading space.
func (t migrationTables) onCluster() string {
	if t.cluster == "" {
		return ""
	}
	return " ON CLUSTER " + quoteIdent(t.cluster)
}

// quoteIdent quotes name as a backtick identifier.
func quoteIdent(name string) string {
	return "`" + strings.NewReplacer(`\`, `\\`, "`", "\\`").Replace(name) + "`"
}

const tierColumns = "fingerprint, bucket, first, last, count, sum, var, min, max, resets, reset_drop, changes, stale_at"

// msBounds selects the timestamps in (from, to].
func msBounds(column string, from, to time.Time) string {
	return fmt.Sprintf("%s > fromUnixTimestamp64Milli(%d) AND %s <= fromUnixTimestamp64Milli(%d)",
		column, from.UnixMilli(), column, to.UnixMilli())
}

// countRawSQL counts the raw samples of the unit's hour, then its non-stale ones.
func (t migrationTables) countRawSQL(u migrationUnit) string {
	return fmt.Sprintf("SELECT count(), countIf(%s) FROM %s FINAL PREWHERE %s", nonStale, t.raw, msBounds("timestamp", u.from, u.to))
}

// splitWhere selects the series of part of a split into parts, or every series when parts ≤ 1.
// Part p holds the fingerprints in [⌊p·2⁶⁴/parts⌋, ⌊(p+1)·2⁶⁴/parts⌋); the last part is open-ended.
func splitWhere(parts, part int) string {
	if parts <= 1 {
		return ""
	}
	where := fmt.Sprintf(" AND fingerprint >= %d", splitBound(parts, part))
	if part < parts-1 {
		where += fmt.Sprintf(" AND fingerprint < %d", splitBound(parts, part+1))
	}
	return where
}

// splitBound is ⌊k·2⁶⁴/parts⌋ for 0 ≤ k < parts.
func splitBound(parts, k int) uint64 {
	q, _ := bits.Div64(uint64(k), 0, uint64(parts))
	return q
}

// copyRawSQL copies the samples_v3 metric rows of split part whose ms lies in the unit's hour
// into raw.
func (t migrationTables) copyRawSQL(u migrationUnit, parts, part int) string {
	return fmt.Sprintf("INSERT INTO %s (fingerprint, timestamp, value) "+
		"SELECT fingerprint, fromUnixTimestamp64Milli(intDiv(timestamp_ns, 1000000)) AS timestamp, value "+
		"FROM %s WHERE %s AND timestamp_ns >= %d AND timestamp_ns < %d%s",
		t.raw, t.samples, legacyMetricRows, (u.from.UnixMilli()+1)*msNs, (u.to.UnixMilli()+1)*msNs,
		splitWhere(parts, part))
}

// source15sSQL turns each metrics_15s metric row of split part starting in [from, to) into one
// sample 1 ms after its start, carrying its last value.
func (t migrationTables) source15sSQL(u migrationUnit, parts, part int) string {
	return fmt.Sprintf("SELECT fingerprint, fromUnixTimestamp64Milli(intDiv(timestamp_ns, 1000000) + 1) AS timestamp, "+
		"argMaxMerge(last) AS value FROM %s WHERE %s AND timestamp_ns >= %d AND timestamp_ns < %d%s "+
		"GROUP BY fingerprint, timestamp_ns",
		t.rollup, legacyMetricRows, u.from.UnixNano(), u.to.UnixNano(), splitWhere(parts, part))
}

// count15sSQL counts the 15s unit's source samples, then its non-stale ones.
func (t migrationTables) count15sSQL(u migrationUnit) string {
	return fmt.Sprintf("SELECT count(), countIf(%s) FROM (%s)", nonStale, t.source15sSQL(u, 1, 0))
}

// rollup15sHoursSQL lists, newest first, the hours [H − 1h, H) holding metrics_15s metric rows
// that start before edge and, with lifetime set, at or after it; each as H in whole hours.
func (t migrationTables) rollup15sHoursSQL(lifetime, edge time.Time) string {
	where := fmt.Sprintf("%s AND timestamp_ns < %d", legacyMetricRows, edge.UnixNano())
	if !lifetime.IsZero() {
		where += fmt.Sprintf(" AND timestamp_ns >= %d", lifetime.UnixNano())
	}
	return fmt.Sprintf("SELECT DISTINCT toInt64(intDiv(timestamp_ns, %d) + 1) AS hour FROM %s WHERE %s ORDER BY hour DESC",
		int64(time.Hour), t.rollup, where)
}

// countTierSQL counts the tier's buckets of the unit's hour.
func (t migrationTables) countTierSQL(tier metricTier, u migrationUnit) string {
	return fmt.Sprintf("SELECT count() FROM %s WHERE %s", t.tier(tier), msBounds("bucket", u.from, u.to))
}

// checkTierSQL sums the samples counted in the tier's buckets of the unit's hour.
func (t migrationTables) checkTierSQL(tier metricTier, u migrationUnit) string {
	return fmt.Sprintf("SELECT sum(count) FROM %s WHERE %s", t.tier(tier), msBounds("bucket", u.from, u.to))
}

// deleteTierSQL deletes the tier's buckets of the unit's hour from the local tier, on every node
// of a cluster.
func (t migrationTables) deleteTierSQL(tier metricTier, u migrationUnit) string {
	return fmt.Sprintf("DELETE FROM %s%s WHERE %s", tier.Table, t.onCluster(), msBounds("bucket", u.from, u.to))
}

// rebuildTierSQL aggregates the raw samples of split part in the unit's hour into the tier.
func (t migrationTables) rebuildTierSQL(tier metricTier, u migrationUnit, parts, part int) string {
	return t.tierRebuildSQL(tier, fmt.Sprintf("%s FINAL PREWHERE %s%s", t.raw, msBounds("timestamp", u.from, u.to), splitWhere(parts, part)))
}

// rebuild15sTierSQL aggregates the 15s unit's source samples of split part into the tier.
func (t migrationTables) rebuild15sTierSQL(tier metricTier, u migrationUnit, parts, part int) string {
	return t.tierRebuildSQL(tier, "("+t.source15sSQL(u, parts, part)+")")
}

// tierRebuildSQL aggregates the samples of from into the tier. Each sample's predecessor is the
// latest earlier non-stale sample of its series in from.
func (t migrationTables) tierRebuildSQL(tier metricTier, from string) string {
	input := "(SELECT fingerprint, timestamp, value, " +
		"prev.1 AS prev_timestamp, prev.2 AS prev_value, 1 AS aggregate " +
		"FROM (SELECT fingerprint, timestamp, value, " +
		"argMaxIf((timestamp, value), timestamp, " + nonStale + ") " +
		"OVER (PARTITION BY fingerprint ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev " +
		"FROM " + from + "))"
	return fmt.Sprintf("INSERT INTO %s (%s) %s", t.tier(tier), tierColumns, tierAggregation(tier, input))
}

// existsSQL reads whether a table exists.
func existsSQL(table string) string {
	return "EXISTS TABLE " + table
}

// seriesSQL copies each metric series seen on or after since's day, or every one when since
// is zero. With rollup set, first_seen reaches back to the series' oldest metrics_15s row.
func (t migrationTables) seriesSQL(rollup bool, since time.Time) string {
	firstSeen := "toDateTime64(first_day, 3, 'UTC')"
	join := ""
	if rollup {
		firstSeen = fmt.Sprintf("if(rollup_first_ns > 0, least(%s, "+
			"fromUnixTimestamp64Milli(intDiv(rollup_first_ns, 1000000), 'UTC')), %s)", firstSeen, firstSeen)
		join = fmt.Sprintf(" LEFT JOIN (SELECT fingerprint, min(timestamp_ns) AS rollup_first_ns FROM %s "+
			"WHERE %s GROUP BY fingerprint) AS rollup USING fingerprint", t.rollup, legacyMetricRows)
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
		"FROM %s WHERE %s GROUP BY fingerprint) AS series%s", t.seriesOut, firstSeen, t.series, where, join)
}

// metadataSQL copies the latest metadata of each metric family.
func (t migrationTables) metadataSQL() string {
	return fmt.Sprintf("INSERT INTO %s (name, type, help, unit, updated_at) "+
		"SELECT name, JSONExtractString(latest, 'type') AS type, JSONExtractString(latest, 'help') AS help, "+
		"JSONExtractString(latest, 'unit') AS unit, fromUnixTimestamp64Milli(intDiv(latest_ns, 1000000)) AS updated_at "+
		"FROM (SELECT name, argMax(metadata, updated_at_ns) AS latest, max(updated_at_ns) AS latest_ns "+
		"FROM %s WHERE %s AND metadata != '' GROUP BY name)", t.metadataOut, t.series, legacyMetricRows)
}

// unitQueryID is the query id each statement of a unit runs under.
func unitQueryID(db string, u migrationUnit) string {
	return fmt.Sprintf("metric_migrate-%s-%s-%d", db, u.kind, u.to.UnixMilli())
}

// runQueryID is the query id of the run's series or metadata statement op.
func runQueryID(db, op string) string {
	return fmt.Sprintf("metric_migrate-%s-%s", db, op)
}

// killSQL kills the statements running under queryID and waits for them to stop.
func killSQL(queryID string) string {
	return fmt.Sprintf("KILL QUERY WHERE query_id = %s SYNC", quoteString(queryID))
}

// killClusterSQL kills, on every node of the run's cluster, the statements started for queryID
// and waits for them to stop.
func (t migrationTables) killClusterSQL(queryID string) string {
	return fmt.Sprintf("KILL QUERY%s WHERE initial_query_id = %s SYNC", t.onCluster(), quoteString(queryID))
}

// quoteString quotes s as a string literal.
func quoteString(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}
