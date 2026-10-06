package maintenance

import (
	"strings"
	"testing"
)

func containsAll(t *testing.T, what, sql string, parts ...string) {
	t.Helper()
	for _, p := range parts {
		if !strings.Contains(sql, p) {
			t.Errorf("%s lacks %q:\n%s", what, p, sql)
		}
	}
}

// (09-30 00:00, 10-01 00:00] in Unix ms.
const chunkFromMs, chunkToMs = "1790726400000", "1790812800000"

func TestAChunkCopiesMetricRowsWithTheirPreviousNonStaleSample(t *testing.T) {
	sql := chunkSQL(localImportTables, at("2026-09-30 00:00"), at("2026-10-01 00:00"))
	containsAll(t, "chunk", sql,
		"INSERT INTO metric_samples_in (fingerprint, timestamp, value, prev_timestamp, prev_value, aggregate)",
		"prev.1 AS prev_timestamp, prev.2 AS prev_value, prev_timestamp < timestamp AS aggregate",
		"argMaxIf((timestamp, value), (timestamp, -timestamp_ns), reinterpretAsUInt64(value) != 0x7ff0000000000002) "+
			"OVER (PARTITION BY fingerprint ORDER BY timestamp_ns ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev",
		"fromUnixTimestamp64Milli(intDiv(timestamp_ns, 1000000)) AS timestamp",
		"FROM samples_v3 WHERE type IN (2, 0)",
		// the window starts a day before the chunk: ms in (09-29, 10-01]
		"timestamp_ns >= 1790640000001000000 AND timestamp_ns < 1790812800001000000",
		"WHERE timestamp > fromUnixTimestamp64Milli("+chunkFromMs+") AND timestamp <= fromUnixTimestamp64Milli("+chunkToMs+")",
	)
	if strings.Contains(sql, "lagInFrame") {
		t.Errorf("chunk takes the previous row, stale or not:\n%s", sql)
	}
}

func TestASpanTurnsEachRollupRowBelowTheFloorIntoOneSample(t *testing.T) {
	sql := spanSQL(localImportTables, at("2026-09-22 00:00"), at("2026-09-29 00:00"), at("2026-09-29 00:00"))
	containsAll(t, "span", sql,
		"INSERT INTO metric_samples_in (fingerprint, timestamp, value, prev_timestamp, prev_value, aggregate)",
		"argMaxIf((timestamp, value), (timestamp, -timestamp_ns), reinterpretAsUInt64(value) != 0x7ff0000000000002) "+
			"OVER (PARTITION BY fingerprint ORDER BY timestamp_ns ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev",
		"SELECT fingerprint, timestamp_ns, fromUnixTimestamp64Milli(intDiv(timestamp_ns, 1000000)) AS timestamp, "+
			"argMaxMerge(last) AS value FROM metrics_15s WHERE type IN (2, 0)",
		// a day before the span, and only rows whose 15s lie wholly below the floor
		"timestamp_ns > 1789948800000000000 AND timestamp_ns <= 1790640000000000000 AND timestamp_ns < 1790640000000000000",
		"GROUP BY fingerprint, timestamp_ns",
		"WHERE timestamp > fromUnixTimestamp64Milli(1790035200000) AND timestamp <= fromUnixTimestamp64Milli(1790640000000)",
	)
}

func TestTheTailCopiesRowsAboveTheWatermarkWithAnHourOfPredecessors(t *testing.T) {
	sql := tailSQL(localImportTables, 1790931600000000000, 1790935200000000000)
	containsAll(t, "tail", sql,
		"argMaxIf((timestamp, value), (timestamp, -timestamp_ns), reinterpretAsUInt64(value) != 0x7ff0000000000002) "+
			"OVER (PARTITION BY fingerprint ORDER BY timestamp_ns ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev",
		"FROM samples_v3 WHERE type IN (2, 0) AND timestamp_ns > 1790928000000000000 AND timestamp_ns <= 1790935200000000000",
		"WHERE timestamp_ns > 1790931600000000000",
	)
}

func TestARedoneUnitDeletesItsBucketsFromBothTiers(t *testing.T) {
	got := redoSQL(localImportTables, importUnit{kind: "chunk", from: at("2026-09-30 00:00"), to: at("2026-10-01 00:00")})
	bounds := " WHERE bucket > fromUnixTimestamp64Milli(" + chunkFromMs + ") AND bucket <= fromUnixTimestamp64Milli(" + chunkToMs + ")"
	sameStrings(t, "redo", got, []string{"DELETE FROM metrics_5m" + bounds, "DELETE FROM metrics_1h" + bounds})
}

func TestSeriesAndMetadataComeFromMetricRowsOnly(t *testing.T) {
	containsAll(t, "series", seriesSQL(localImportTables, false),
		"INSERT INTO metric_series (name, fingerprint, labels, first_seen, last_seen)",
		"CAST(JSONExtractKeysAndValues(series_labels, 'String') AS Map(LowCardinality(String), String)) AS labels",
		"toDateTime64(first_day, 3, 'UTC') AS first_seen",
		"toDateTime64(last_day + INTERVAL 1 DAY, 3, 'UTC') AS last_seen",
		"min(date) AS first_day, max(date) AS last_day FROM time_series WHERE type IN (2, 0) GROUP BY fingerprint",
	)
	if sql := seriesSQL(localImportTables, false); strings.Contains(sql, "metrics_15s") {
		t.Errorf("series reads the rollup when it is absent:\n%s", sql)
	}
	containsAll(t, "metadata", metadataSQL(localImportTables),
		"INSERT INTO metric_metadata (name, type, help, unit, updated_at)",
		"argMax(metadata, updated_at_ns) AS latest",
		"FROM time_series WHERE type IN (2, 0) AND metadata != '' GROUP BY name",
	)
}

func TestASeriesIsSeenFromItsOldestRollupRowWhenThatIsEarlier(t *testing.T) {
	containsAll(t, "series", seriesSQL(localImportTables, true),
		"LEFT JOIN (SELECT fingerprint, min(timestamp_ns) AS rollup_first_ns FROM metrics_15s "+
			"WHERE type IN (2, 0) GROUP BY fingerprint) AS rollup USING fingerprint",
		"if(rollup_first_ns > 0, least(toDateTime64(first_day, 3, 'UTC'), "+
			"fromUnixTimestamp64Milli(intDiv(rollup_first_ns, 1000000), 'UTC')), toDateTime64(first_day, 3, 'UTC')) AS first_seen",
	)
}

func TestEachUnitRunsUnderItsOwnQueryIDAndARedoKillsItFirst(t *testing.T) {
	u := importUnit{kind: "chunk", from: at("2026-09-30 00:00"), to: at("2026-10-01 00:00")}
	if got := unitQueryID("cloki", u.name()); got != "metric_import-cloki-chunk:"+chunkToMs {
		t.Errorf("query id = %s", got)
	}
	if got := unitQueryID("cloki", "tail"); got != "metric_import-cloki-tail" {
		t.Errorf("tail query id = %s", got)
	}
	want := "KILL QUERY WHERE initial_query_id = 'metric_import-cloki-chunk:" + chunkToMs + "' SYNC"
	if got := killSQL(localImportTables, unitQueryID("cloki", u.name())); got != want {
		t.Errorf("kill = %s, want %s", got, want)
	}
}

var clusterTables = clusterImportTables("c1")

const syncInsert = " SETTINGS insert_distributed_sync = 1"

func TestOnAClusterEachCopyRunsThroughTheDistributedTablesAndWaitsForItsShards(t *testing.T) {
	for name, tc := range map[string]struct {
		sql   string
		parts []string
	}{
		"chunk":    {chunkSQL(clusterTables, at("2026-09-30 00:00"), at("2026-10-01 00:00")), []string{"INSERT INTO metric_samples_in_dist ", "FROM samples_v3_dist WHERE"}},
		"span":     {spanSQL(clusterTables, at("2026-09-22 00:00"), at("2026-09-29 00:00"), at("2026-09-29 00:00")), []string{"INSERT INTO metric_samples_in_dist ", "FROM metrics_15s_dist WHERE"}},
		"tail":     {tailSQL(clusterTables, 1790931600000000000, 1790935200000000000), []string{"INSERT INTO metric_samples_in_dist ", "FROM samples_v3_dist WHERE"}},
		"series":   {seriesSQL(clusterTables, true), []string{"INSERT INTO metric_series_dist ", "FROM time_series_dist WHERE", "FROM metrics_15s_dist WHERE"}},
		"metadata": {metadataSQL(clusterTables), []string{"INSERT INTO metric_metadata_dist ", "FROM time_series_dist WHERE"}},
	} {
		containsAll(t, name, tc.sql, tc.parts...)
		if !strings.HasSuffix(tc.sql, syncInsert) {
			t.Errorf("%s does not wait for its shards:\n%s", name, tc.sql)
		}
		if strings.Contains(tc.sql, "GLOBAL") {
			t.Errorf("%s ships a set across shards:\n%s", name, tc.sql)
		}
	}
	if sql := chunkSQL(localImportTables, at("2026-09-30 00:00"), at("2026-10-01 00:00")); strings.Contains(sql, "SETTINGS") {
		t.Errorf("a single-node chunk carries settings:\n%s", sql)
	}
}

func TestOnAClusterARedoDeletesAndKillsOnEveryNode(t *testing.T) {
	got := redoSQL(clusterTables, importUnit{kind: "chunk", from: at("2026-09-30 00:00"), to: at("2026-10-01 00:00")})
	bounds := " WHERE bucket > fromUnixTimestamp64Milli(" + chunkFromMs + ") AND bucket <= fromUnixTimestamp64Milli(" + chunkToMs + ")"
	sameStrings(t, "redo", got, []string{
		"DELETE FROM metrics_5m ON CLUSTER `c1`" + bounds, "DELETE FROM metrics_1h ON CLUSTER `c1`" + bounds})
	want := "KILL QUERY ON CLUSTER `c1` WHERE initial_query_id = 'metric_import-cloki-tail' SYNC"
	if got := killSQL(clusterTables, unitQueryID("cloki", "tail")); got != want {
		t.Errorf("kill = %s, want %s", got, want)
	}
}

func TestOnAClusterTheRecordsGoThroughTheDistributedSettings(t *testing.T) {
	read := recordsSQL(clusterTables)
	containsAll(t, "records", read,
		"SELECT argMax(name, inserted_at), argMax(value, inserted_at) FROM settings_dist WHERE type = $1 GROUP BY fingerprint")
	if strings.Contains(read, "FINAL") {
		t.Errorf("records read with FINAL:\n%s", read)
	}
	put := recordPutSQL(clusterTables)
	containsAll(t, "record write", put, "INSERT INTO settings_dist (fingerprint, type, name, value, inserted_at)")
	if !strings.HasSuffix(put, syncInsert) {
		t.Errorf("a record write does not wait for its shard:\n%s", put)
	}
	containsAll(t, "migration time", migrationTimeSQL(clusterTables), "FROM settings_dist WHERE")
}
