package metricread

import (
	"strings"
	"testing"
)

// rowsOf returns the rows CTE of a pushdown's SQL.
func rowsOf(t *testing.T, sql string) string {
	t.Helper()
	i := strings.Index(sql, "rows AS (")
	j := strings.Index(sql, ") SELECT fingerprint, ")
	if i < 0 || j < 0 {
		t.Fatalf("no rows CTE in %s", sql)
	}
	return sql[i+len("rows AS (") : j]
}

const tierBuckets5m = "SELECT fingerprint, bucket, " +
	"minIfMerge(first) AS b_first, maxIfMerge(last) AS b_last, " +
	"sum(count) AS b_count, sum(sum) AS b_sum, sum(sum_sq) AS b_sum_sq, " +
	"minIfMerge(min) AS b_min, maxIfMerge(max) AS b_max, " +
	"sum(resets) AS b_resets, sum(reset_drop) AS b_reset_drop, sum(changes) AS b_changes, " +
	"max(stale_at) AS b_stale_at " +
	"FROM metrics_5m " +
	"WHERE fingerprint IN (SELECT fingerprint FROM fp) " +
	"AND bucket > fromUnixTimestamp64Milli(start_ms - range_ms) " +
	"AND bucket <= fromUnixTimestamp64Milli(end_ms) " +
	"GROUP BY fingerprint, bucket " +
	"HAVING b_count > 0"

func tierRows(header string) string {
	return header +
		"intDiv(end_ms - start_ms, step_ms) + 1 AS n_steps, " +
		"prev_b_ms > 0 AND prev_b_ms > start_ms + k * step_ms - range_ms AS prev_in " +
		"SELECT fingerprint, start_ms + k * step_ms AS t_ms, " +
		"min(b_first) AS first, " +
		"max(b_last) AS last, " +
		"sum(b_count) AS count, " +
		"if(isFinite(sum(b_sum)), sumKahan(b_sum), sum(b_sum)) AS sum, " +
		"if(min(b_min) > max(b_max), nan, min(b_min)) AS min, " +
		"if(min(b_min) > max(b_max), nan, max(b_max)) AS max, " +
		"sum(b_resets) + countIf(prev_in AND b_first.2 < prev_last.2) AS resets, " +
		"sum(b_reset_drop) + sumIf(prev_last.2, prev_in AND b_first.2 < prev_last.2) AS reset_drop, " +
		"sum(b_changes) + countIf(prev_in AND b_first.2 != prev_last.2 " +
		"AND NOT (isNaN(b_first.2) AND isNaN(prev_last.2))) AS changes, " +
		"max(b_stale_at) AS stale_at, " +
		"argMax(if(prev_in, prev_last, b_first), b_ms) AS penult, " +
		"sum(b_sum_sq) / count - pow(sum / count, 2) AS dev_sq, " +
		"if(dev_sq < 0, 0., dev_sq) AS var " +
		"FROM (SELECT fingerprint, b_first, b_last, b_count, b_sum, b_sum_sq, b_min, b_max, " +
		"b_resets, b_reset_drop, b_changes, b_stale_at, " +
		"toUnixTimestamp64Milli(bucket) AS b_ms, " +
		"lagInFrame(b_last) OVER w AS prev_last, " +
		"lagInFrame(b_ms) OVER w AS prev_b_ms, " +
		"greatest(0, if(b_ms >= start_ms, intDiv(b_ms - start_ms + step_ms - 1, step_ms), " +
		"-intDiv(start_ms - b_ms, step_ms))) AS k_min, " +
		"least(n_steps - 1, intDiv(b_ms - w_ms + range_ms - start_ms, step_ms)) AS k_max " +
		"FROM (" + tierBuckets5m + ") " +
		"WINDOW w AS (PARTITION BY fingerprint ORDER BY bucket ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) " +
		"ARRAY JOIN range(k_min, k_max + 1) AS k " +
		"GROUP BY fingerprint, k " +
		"HAVING count > 0"
}

func TestTierPushdownSQLMergesWholeBucketsAtAnAlignedRead(t *testing.T) {
	grid := Grid{StartMs: 1767225600000, EndMs: 1767226200000, StepMs: 300000}
	sql := PushdownSQL(Pushdown{Grid: grid, Func: "rate", RangeMs: 600000, Matchers: probeSelector(), Tier: Tier5m})
	want := tierRows("WITH 1767225600000 AS start_ms, 1767226200000 AS end_ms, 300000 AS step_ms, " +
		"600000 AS range_ms, 300000 AS w_ms, ")
	if got := rowsOf(t, sql); got != want {
		t.Errorf("rows\ngot  %s\nwant %s", got, want)
	}
	if !strings.HasPrefix(sql, "WITH fp AS (") || !strings.Contains(sql, "WITH 600000 AS range_ms, 1 AS is_counter, 1 AS per_second, ") {
		t.Errorf("not evaluated as rate[10m] at the query's own timestamps: %s", sql)
	}
}

func TestTierPushdownSQLSnapsTimestampsAndWidensRanges(t *testing.T) {
	for _, tc := range []struct {
		name      string
		grid      Grid
		rangeMs   int64
		wantRows  string
		wantRange string
		wantStamp string
	}{
		{"1m range at 1m steps", Grid{StartMs: 1767225660000, EndMs: 1767226200000, StepMs: 60000}, 60000,
			"WITH 1767225600000 AS start_ms, 1767226200000 AS end_ms, 300000 AS step_ms, 300000 AS range_ms, 300000 AS w_ms, ",
			"WITH 300000 AS range_ms, ",
			"WITH 1767225660000 AS start_ms, 60000 AS step_ms, 300000 AS w_ms, intDiv(1767226200000 - start_ms, step_ms) + 1 AS n_steps "},
		{"7m range at 10m steps off the grid", Grid{StartMs: 1767225720000, EndMs: 1767227520000, StepMs: 600000}, 420000,
			"WITH 1767225600000 AS start_ms, 1767227400000 AS end_ms, 600000 AS step_ms, 600000 AS range_ms, 300000 AS w_ms, ",
			"WITH 600000 AS range_ms, ",
			"WITH 1767225720000 AS start_ms, 600000 AS step_ms, 300000 AS w_ms, intDiv(1767227520000 - start_ms, step_ms) + 1 AS n_steps "},
		{"instant", Grid{StartMs: 1767226020000, EndMs: 1767226020000}, 600000,
			"WITH 1767225900000 AS start_ms, 1767225900000 AS end_ms, 1 AS step_ms, 600000 AS range_ms, 300000 AS w_ms, ",
			"WITH 600000 AS range_ms, ",
			"WITH 1767226020000 AS start_ms, 1 AS step_ms, 300000 AS w_ms, intDiv(1767226020000 - start_ms, step_ms) + 1 AS n_steps "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := PushdownSQL(Pushdown{Grid: tc.grid, Func: "increase", RangeMs: tc.rangeMs, Matchers: probeSelector(), Tier: Tier5m})
			if !strings.HasPrefix(sql, tc.wantStamp) {
				t.Fatalf("not stamped at the query's timestamps: %s", sql)
			}
			const tail = "SELECT fingerprint, labels, start_ms + k * step_ms AS t_ms, value FROM (" +
				"SELECT fingerprint, labels, t_ms AS tier_ms, value FROM (WITH fp AS ("
			if !strings.HasPrefix(sql[len(tc.wantStamp):], tail) {
				t.Errorf("stamp\ngot  %s\nwant %s", sql[len(tc.wantStamp):], tail)
			}
			const join = ")) ARRAY JOIN range(greatest(0, if(tier_ms >= start_ms, " +
				"intDiv(tier_ms - start_ms + step_ms - 1, step_ms), -intDiv(start_ms - tier_ms, step_ms))), " +
				"least(n_steps - 1, intDiv(tier_ms + w_ms - 1 - start_ms, step_ms)) + 1) AS k " +
				"ORDER BY fingerprint, t_ms"
			if !strings.HasSuffix(sql, join) {
				t.Errorf("stamp join\ngot  %s\nwant suffix %s", sql, join)
			}
			if got := rowsOf(t, sql); got != tierRows(tc.wantRows) {
				t.Errorf("rows\ngot  %s\nwant %s", got, tierRows(tc.wantRows))
			}
			if !strings.Contains(sql, tc.wantRange+"1 AS is_counter, 0 AS per_second") {
				t.Errorf("value not over the widened range %q: %s", tc.wantRange, sql)
			}
		})
	}
}

func TestTierPushdownSQLInstantSelectorReadsTheBucketsLast(t *testing.T) {
	at := int64(1767229200000)
	sql := PushdownSQL(Pushdown{Grid: Grid{StartMs: at, EndMs: at}, RangeMs: 300000, Matchers: probeSelector(), Tier: Tier1h})
	want := "WITH 1767229200000 AS start_ms, 1767229200000 AS end_ms, 1 AS step_ms, 300000 AS lookback_ms " +
		"SELECT fingerprint, toUnixTimestamp64Milli(bucket) AS t_ms, b_last AS last, b_stale_at AS stale_at " +
		"FROM (SELECT fingerprint, bucket, maxIfMerge(last) AS b_last, max(stale_at) AS b_stale_at FROM metrics_1h " +
		"WHERE fingerprint IN (SELECT fingerprint FROM fp) " +
		"AND bucket >= fromUnixTimestamp64Milli(start_ms) AND bucket <= fromUnixTimestamp64Milli(end_ms) " +
		"AND (toUnixTimestamp64Milli(bucket) - start_ms) % step_ms = 0 " +
		"GROUP BY fingerprint, bucket) " +
		"WHERE toUnixTimestamp64Milli(last.1) > t_ms - lookback_ms"
	if got := rowsOf(t, sql); got != want {
		t.Errorf("rows\ngot  %s\nwant %s", got, want)
	}
	points, lbls := pointsOf(t, sql)
	if points != "SELECT fingerprint, t_ms, last.2 AS value FROM rows WHERE last.1 > stale_at" || lbls != "label_set" {
		t.Errorf("points %s with labels %s", points, lbls)
	}
}

func TestTierSamplesSQL(t *testing.T) {
	got := TierSamplesSQL(Window{FromMs: 1767225600000, ToMs: 1767226200000}, Tier5m, probeSelector())
	want := "WITH fp AS (SELECT fingerprint, any(labels) AS label_set FROM metric_series " +
		"WHERE (name = 'x') " +
		"AND last_seen >= fromUnixTimestamp64Milli(1767223800000) " +
		"AND first_seen <= fromUnixTimestamp64Milli(1767226200000) " +
		"GROUP BY fingerprint) " +
		"SELECT fingerprint, if(marker, b_stale_at, b_last.1) AS timestamp, " +
		"if(marker, reinterpretAsFloat64(toUInt64(9218868437227405314)), b_last.2) AS value " +
		"FROM (SELECT fingerprint, bucket, maxIfMerge(last) AS b_last, max(stale_at) AS b_stale_at FROM metrics_5m " +
		"WHERE fingerprint IN (SELECT fingerprint FROM fp) " +
		"AND bucket > fromUnixTimestamp64Milli(1767225600000) " +
		"AND bucket < fromUnixTimestamp64Milli(1767226500000) " +
		"GROUP BY fingerprint, bucket) " +
		"ARRAY JOIN [0, 1] AS marker " +
		"WHERE if(marker, b_stale_at > b_last.1, b_last.1 > toDateTime64(0, 3)) " +
		"AND timestamp > fromUnixTimestamp64Milli(1767225600000) " +
		"AND timestamp <= fromUnixTimestamp64Milli(1767226200000) " +
		"ORDER BY fingerprint, timestamp"
	if got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}
