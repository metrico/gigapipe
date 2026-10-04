package metricread

import (
	"strings"
	"testing"
)

func TestSubBucketWidth(t *testing.T) {
	const s, m, h = int64(1000), int64(60000), int64(3600000)
	for _, tc := range []struct {
		name          string
		step, rangeMs int64
		want          int64
	}{
		{"5m at 2m", 2 * m, 5 * m, m},
		{"1h at 2m", 2 * m, h, 2 * m},
		{"10m at 3m", 3 * m, 10 * m, m},
		{"1h at 30s", 30 * s, h, 30 * s},
		{"lookback at 2m", 2 * m, 5 * m, m},
		{"5m at 15s fans out each sample", 15 * s, 5 * m, 0},
		{"30s at 20s fans out each sample", 20 * s, 30 * s, 0},
		{"range of one step reaches one window", m, m, 0},
		{"range below the step reaches one window", 5 * m, m, 0},
		{"instant", 0, 5 * m, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := Pushdown{Grid: Grid{StartMs: 1767225600000, EndMs: 1767225600000 + 10*tc.step, StepMs: tc.step}, RangeMs: tc.rangeMs}
			if got := subBucketMs(p); got != tc.want {
				t.Errorf("subBucketMs(step %d, range %d) = %d, want %d", tc.step, tc.rangeMs, got, tc.want)
			}
		})
	}
}

// probeSubBuckets groups the probe's raw samples into 1m sub-buckets carrying aggs; prev adds each
// sample's predecessor and its pairing inside the sub-bucket.
func probeSubBuckets(prev bool, aggs, having string) string {
	window := ""
	if prev {
		window = ", maxIf((timestamp, value), NOT stale) OVER (PARTITION BY fingerprint ORDER BY timestamp " +
			"ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev, " +
			"toUnixTimestamp64Milli(prev.1) AS prev_ms, prev.2 AS prev_value, " +
			"NOT stale AND prev_ms > start_ms + (j - 1) * w_ms AS paired"
	}
	return "SELECT fingerprint, start_ms + j * w_ms AS bucket, " + aggs + " " +
		"FROM (SELECT fingerprint, timestamp, value, " +
		"toUnixTimestamp64Milli(timestamp) AS ts_ms, " +
		"reinterpretAsUInt64(value) = 0x7ff0000000000002 AS stale, " +
		"if(ts_ms >= start_ms, intDiv(ts_ms - start_ms + w_ms - 1, w_ms), -intDiv(start_ms - ts_ms, w_ms)) AS j" +
		window + " " +
		"FROM (SELECT fingerprint, timestamp, value FROM metric_samples FINAL " +
		"WHERE " + localSeries(1767223500000, 1767226200000) + " " +
		"AND timestamp > fromUnixTimestamp64Milli(start_ms - range_ms) " +
		"AND timestamp <= fromUnixTimestamp64Milli(end_ms))) " +
		"GROUP BY fingerprint, j" + having
}

const subBucketHeader = "WITH 1767225600000 AS start_ms, 1767226200000 AS end_ms, 60000 AS step_ms, " +
	"300000 AS range_ms, 60000 AS w_ms, intDiv(end_ms - start_ms, step_ms) + 1 AS n_steps"

// fanOut places each sub-bucket on the steps whose window holds it and groups per step.
const fanOut = "greatest(0, if(b_ms >= start_ms, intDiv(b_ms - start_ms + step_ms - 1, step_ms), " +
	"-intDiv(start_ms - b_ms, step_ms))) AS k_min, " +
	"least(n_steps - 1, intDiv(b_ms - w_ms + range_ms - start_ms, step_ms)) AS k_max "

func TestRawPushdownSQLReadsSubBucketsAsATierWould(t *testing.T) {
	got := rowsOf(t, PushdownSQL(Pushdown{Grid: probeGrid, Func: "rate", RangeMs: 300000, Matchers: probeSelector()}))
	want := subBucketHeader + ", prev_b_ms > 0 AND prev_b_ms > start_ms + k * step_ms - range_ms AS prev_in " +
		"SELECT fingerprint, start_ms + k * step_ms AS t_ms, " +
		"(min(b_first.1), argMin(b_first.2, b_first.1)) AS first, " +
		"(max(b_last.1), argMax(b_last.2, b_last.1)) AS last, " +
		"sum(b_count) AS count, " +
		"sum(b_reset_drop) + sumIf(prev_last.2, prev_in AND b_first.2 < prev_last.2) AS reset_drop " +
		"FROM (SELECT fingerprint, b_first, b_last, b_count, b_reset_drop, " +
		"bucket AS b_ms, " +
		"lagInFrame(b_last) OVER w AS prev_last, " +
		"lagInFrame(b_ms) OVER w AS prev_b_ms, " +
		fanOut +
		"FROM (" + probeSubBuckets(true,
		"(minIf(timestamp, NOT stale), argMinIf(value, timestamp, NOT stale)) AS b_first, "+
			"(maxIf(timestamp, NOT stale), argMaxIf(value, timestamp, NOT stale)) AS b_last, "+
			"countIf(NOT stale) AS b_count, "+
			"sumIf(prev_value, paired AND value < prev_value) AS b_reset_drop", " HAVING b_count > 0") + ") " +
		"WINDOW w AS (PARTITION BY fingerprint ORDER BY bucket ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) " +
		"ARRAY JOIN range(k_min, k_max + 1) AS k " +
		"GROUP BY fingerprint, k " +
		"HAVING count > 0"
	if got != want {
		t.Errorf("rows\ngot  %s\nwant %s", got, want)
	}
}

func TestSubBucketRowShapeCarriesWhatTheFunctionReads(t *testing.T) {
	rows := func(cols, names string, prev bool, aggs, having string) string {
		return subBucketHeader + " SELECT fingerprint, start_ms + k * step_ms AS t_ms, " + cols + " " +
			"FROM (SELECT fingerprint, " + names + ", bucket AS b_ms, " + fanOut +
			"FROM (" + probeSubBuckets(prev, aggs, having) + ")) " +
			"ARRAY JOIN range(k_min, k_max + 1) AS k " +
			"GROUP BY fingerprint, k " +
			"HAVING count > 0"
	}
	const (
		last    = "(maxIf(timestamp, NOT stale), argMaxIf(value, timestamp, NOT stale)) AS b_last"
		count   = "countIf(NOT stale) AS b_count"
		lastCol = "(max(b_last.1), argMax(b_last.2, b_last.1)) AS last"
	)
	for _, tc := range []struct {
		name, fn, want string
	}{
		{"a sub-bucket of stale markers alone keeps the selector's stale_at", "",
			rows(lastCol+", sum(b_count) AS count, max(b_stale_at) AS stale_at", "b_last, b_count, b_stale_at", false,
				last+", "+count+", maxIf(timestamp, stale) AS b_stale_at", "")},
		{"avg reads count and a compensated sum", "avg_over_time",
			rows("sum(b_count) AS count, if(isFinite(sum(b_sum)), sumKahan(b_sum), sum(b_sum)) AS sum", "b_count, b_sum", false,
				count+", if(isFinite(sumIf(value, NOT stale)), sumKahanIf(value, NOT stale), sumIf(value, NOT stale)) AS b_sum",
				" HAVING b_count > 0")},
		{"min maps NaN out of the way as the tier views do", "min_over_time",
			rows("sum(b_count) AS count, if(min(b_min) > max(b_max), nan, min(b_min)) AS min", "b_count, b_min, b_max", false,
				count+", minIf(if(isNaN(value), inf, value), NOT stale) AS b_min, maxIf(if(isNaN(value), -inf, value), NOT stale) AS b_max",
				" HAVING b_count > 0")},
		{"variance merges each sub-bucket's state", "stddev_over_time",
			rows("sum(b_count) AS count, varPopStableIfMerge(b_var) AS var", "b_count, b_var", false,
				count+", varPopStableIfState(value, NOT stale) AS b_var", " HAVING b_count > 0")},
		{"irate's penult is the predecessor of the window's last sample", "irate",
			rows(lastCol+", sum(b_count) AS count, argMax(b_penult, b_ms) AS penult", "b_last, b_count, b_penult", true,
				last+", "+count+", argMaxIf(prev, timestamp, NOT stale) AS b_penult", " HAVING b_count > 0")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := rowsOf(t, PushdownSQL(Pushdown{Grid: probeGrid, Func: tc.fn, RangeMs: 300000, Matchers: probeSelector()}))
			if got != tc.want {
				t.Errorf("rows\ngot  %s\nwant %s", got, tc.want)
			}
		})
	}
}

func TestSubBucketsPairAcrossTheirEdgesForResetsAndChanges(t *testing.T) {
	for fn, want := range map[string]string{
		"resets": "sum(b_resets) + countIf(prev_in AND b_first.2 < prev_last.2) AS resets",
		"changes": "sum(b_changes) + countIf(prev_in AND b_first.2 != prev_last.2 " +
			"AND NOT (isNaN(b_first.2) AND isNaN(prev_last.2))) AS changes",
	} {
		rows := rowsOf(t, PushdownSQL(Pushdown{Grid: probeGrid, Func: fn, RangeMs: 300000, Matchers: probeSelector()}))
		for _, part := range []string{want, "lagInFrame(b_last) OVER w AS prev_last", "NOT stale AND prev_ms > start_ms + (j - 1) * w_ms AS paired"} {
			if !strings.Contains(rows, part) {
				t.Errorf("%s rows lack %s: %s", fn, part, rows)
			}
		}
	}
}

func TestRawPushdownKeepsTheSamplePathWhereSubBucketsSaveNothing(t *testing.T) {
	for name, p := range map[string]Pushdown{
		"15s step":      {Grid: sampleGrid, Func: "rate", RangeMs: 300000, Matchers: probeSelector()},
		"range of step": {Grid: probeGrid, Func: "rate", RangeMs: 60000, Matchers: probeSelector()},
		"instant":       {Grid: Grid{StartMs: probeGrid.EndMs, EndMs: probeGrid.EndMs}, Func: "rate", RangeMs: 300000, Matchers: probeSelector()},
	} {
		if rows := rowsOf(t, PushdownSQL(p)); rows != rawRowsSQL(p) {
			t.Errorf("%s does not fan out each sample: %s", name, rows)
		}
	}
}
