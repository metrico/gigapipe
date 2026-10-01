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
		"maxIf((timestamp, value), reinterpretAsUInt64(value) != 0x7ff0000000000002) "+
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
		"maxIf((timestamp, value), reinterpretAsUInt64(value) != 0x7ff0000000000002) "+
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
		"maxIf((timestamp, value), reinterpretAsUInt64(value) != 0x7ff0000000000002) "+
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
	containsAll(t, "series", seriesSQL(localImportTables),
		"INSERT INTO metric_series (name, fingerprint, labels, first_seen, last_seen)",
		"CAST(JSONExtractKeysAndValues(series_labels, 'String') AS Map(LowCardinality(String), String)) AS labels",
		"toDateTime64(first_day, 3, 'UTC') AS first_seen",
		"toDateTime64(last_day + INTERVAL 1 DAY, 3, 'UTC') AS last_seen",
		"min(date) AS first_day, max(date) AS last_day FROM time_series WHERE type IN (2, 0) GROUP BY fingerprint",
	)
	containsAll(t, "metadata", metadataSQL(localImportTables),
		"INSERT INTO metric_metadata (name, type, help, unit, updated_at)",
		"argMax(metadata, updated_at_ns) AS latest",
		"FROM time_series WHERE type IN (2, 0) AND metadata != '' GROUP BY name",
	)
}
