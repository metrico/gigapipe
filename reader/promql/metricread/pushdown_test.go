package metricread

import (
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
)

// probeGrid evaluates every minute of the probe's (00:00, 00:10].
var probeGrid = Grid{StartMs: 1767225600000, EndMs: 1767226200000, StepMs: 60000}

func probeSelector() []*labels.Matcher {
	return []*labels.Matcher{matcher(labels.MatchEqual, "__name__", "x")}
}

func TestPushdownSQLRate(t *testing.T) {
	got := PushdownSQL(Pushdown{Grid: probeGrid, Func: "rate", RangeMs: 300000, Matchers: probeSelector()})
	want := "WITH fp AS (SELECT fingerprint, any(labels) AS label_set FROM metric_series " +
		"WHERE (name = 'x') " +
		"AND last_seen >= fromUnixTimestamp64Milli(1767223500000) " +
		"AND first_seen <= fromUnixTimestamp64Milli(1767226200000) " +
		"GROUP BY fingerprint), " +
		"rows AS (WITH 1767225600000 AS start_ms, 1767226200000 AS end_ms, 60000 AS step_ms, 300000 AS range_ms, " +
		"intDiv(end_ms - start_ms, step_ms) + 1 AS n_steps, " +
		"NOT stale AND prev_ms > start_ms + k * step_ms - range_ms AS paired " +
		"SELECT fingerprint, start_ms + k * step_ms AS t_ms, " +
		"minIf((timestamp, value), NOT stale) AS first, " +
		"maxIf((timestamp, value), NOT stale) AS last, " +
		"countIf(NOT stale) AS count, " +
		"if(isFinite(sumIf(value, NOT stale)), sumKahanIf(value, NOT stale), sumIf(value, NOT stale)) AS sum, " +
		"ifNull(minIfOrNull(value, NOT stale AND NOT isNaN(value)), nan) AS min, " +
		"ifNull(maxIfOrNull(value, NOT stale AND NOT isNaN(value)), nan) AS max, " +
		"countIf(paired AND value < prev_value) AS resets, " +
		"sumIf(prev_value, paired AND value < prev_value) AS reset_drop, " +
		"countIf(paired AND value != prev_value AND NOT (isNaN(value) AND isNaN(prev_value))) AS changes, " +
		"maxIf(timestamp, stale) AS stale_at, " +
		"argMaxIf(prev, timestamp, NOT stale) AS penult, " +
		"varPopStableIf(value, NOT stale) AS var " +
		"FROM (SELECT fingerprint, timestamp, value, " +
		"toUnixTimestamp64Milli(timestamp) AS ts_ms, " +
		"reinterpretAsUInt64(value) = 0x7ff0000000000002 AS stale, " +
		"maxIf((timestamp, value), NOT stale) OVER (PARTITION BY fingerprint ORDER BY timestamp " +
		"ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev, " +
		"toUnixTimestamp64Milli(prev.1) AS prev_ms, prev.2 AS prev_value, " +
		"greatest(0, if(ts_ms >= start_ms, intDiv(ts_ms - start_ms + step_ms - 1, step_ms), " +
		"-intDiv(start_ms - ts_ms, step_ms))) AS k_min, " +
		"least(n_steps - 1, intDiv(ts_ms + range_ms - 1 - start_ms, step_ms)) AS k_max " +
		"FROM (SELECT fingerprint, timestamp, value FROM metric_samples " +
		"WHERE " + localSeries(1767223500000, 1767226200000) + " " +
		"AND timestamp > fromUnixTimestamp64Milli(start_ms - range_ms) " +
		"AND timestamp <= fromUnixTimestamp64Milli(end_ms) " +
		"ORDER BY fingerprint, timestamp " +
		"LIMIT 1 BY fingerprint, timestamp)) " +
		"ARRAY JOIN range(k_min, k_max + 1) AS k " +
		"GROUP BY fingerprint, k " +
		"HAVING count > 0) " +
		"SELECT fingerprint, mapFilter((k, v) -> k != '__name__', label_set) AS labels, t_ms, value FROM (" +
		"WITH 300000 AS range_ms, 1 AS is_counter, 1 AS per_second, " +
		"t_ms - range_ms AS window_start_ms, " +
		"toUnixTimestamp64Milli(first.1) AS first_ms, toUnixTimestamp64Milli(last.1) AS last_ms, " +
		"(last_ms - first_ms) / 1000 AS sampled_s, " +
		"sampled_s / (count - 1) AS avg_gap_s, " +
		"(first_ms - window_start_ms) / 1000 AS to_start_s, " +
		"(t_ms - last_ms) / 1000 AS to_end_s, " +
		"if(is_counter, last.2 - first.2 + reset_drop, last.2 - first.2) AS change, " +
		"if(to_start_s >= avg_gap_s * 1.1, avg_gap_s / 2, to_start_s) AS ext_start_s, " +
		"if(is_counter AND change > 0 AND first.2 >= 0 AND sampled_s * (first.2 / change) < ext_start_s, " +
		"sampled_s * (first.2 / change), ext_start_s) AS ext_start_capped_s, " +
		"if(to_end_s >= avg_gap_s * 1.1, avg_gap_s / 2, to_end_s) AS ext_end_s, " +
		"(sampled_s + ext_start_capped_s + ext_end_s) / sampled_s AS factor " +
		"SELECT fingerprint, t_ms, change * (factor / if(per_second, range_ms / 1000, 1)) AS value " +
		"FROM rows WHERE count >= 2) AS points " +
		"INNER JOIN fp USING (fingerprint) " +
		"ORDER BY fingerprint, t_ms"
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

// pointsOf returns the per-series select of a pushdown's SQL and the label set it outputs.
func pointsOf(t *testing.T, sql string) (points, lbls string) {
	t.Helper()
	const head, tail = "SELECT fingerprint, ", ") AS points "
	i := strings.LastIndex(sql, ") "+head)
	j := strings.LastIndex(sql, tail)
	if i < 0 || j < 0 {
		t.Fatalf("no per-series select in %s", sql)
	}
	sel := sql[i+2 : j]
	k := strings.Index(sel, " AS labels, t_ms, value FROM (")
	if k < 0 {
		t.Fatalf("no labels in %s", sel)
	}
	return sel[k+len(" AS labels, t_ms, value FROM ("):], sel[len(head):k]
}

func TestPushdownSQLFunctionValues(t *testing.T) {
	const dropName = "mapFilter((k, v) -> k != '__name__', label_set)"
	extrapolated := func(counter, perSecond int) string {
		return fmt.Sprintf("WITH 600000 AS range_ms, %d AS is_counter, %d AS per_second, ", counter, perSecond)
	}
	for _, tc := range []struct {
		fn         string
		wantPrefix string
		want       string
		wantLabels string
	}{
		{fn: "increase", wantPrefix: extrapolated(1, 0), wantLabels: dropName},
		{fn: "delta", wantPrefix: extrapolated(0, 0), wantLabels: dropName},
		{fn: "irate", want: "SELECT fingerprint, t_ms, if(last.2 < penult.2, last.2, last.2 - penult.2) / " +
			"((toUnixTimestamp64Milli(last.1) - toUnixTimestamp64Milli(penult.1)) / 1000) AS value " +
			"FROM rows WHERE count >= 2", wantLabels: dropName},
		{fn: "idelta", want: "SELECT fingerprint, t_ms, last.2 - penult.2 AS value FROM rows WHERE count >= 2",
			wantLabels: dropName},
		{fn: "resets", want: "SELECT fingerprint, t_ms, toFloat64(resets) AS value FROM rows", wantLabels: dropName},
		{fn: "changes", want: "SELECT fingerprint, t_ms, toFloat64(changes) AS value FROM rows", wantLabels: dropName},
		{fn: "count_over_time", want: "SELECT fingerprint, t_ms, toFloat64(count) AS value FROM rows", wantLabels: dropName},
		{fn: "sum_over_time", want: "SELECT fingerprint, t_ms, sum AS value FROM rows", wantLabels: dropName},
		{fn: "min_over_time", want: "SELECT fingerprint, t_ms, min AS value FROM rows", wantLabels: dropName},
		{fn: "max_over_time", want: "SELECT fingerprint, t_ms, max AS value FROM rows", wantLabels: dropName},
		{fn: "avg_over_time", want: "SELECT fingerprint, t_ms, sum / count AS value FROM rows", wantLabels: dropName},
		{fn: "stdvar_over_time", want: "SELECT fingerprint, t_ms, var AS value FROM rows", wantLabels: dropName},
		{fn: "stddev_over_time", want: "SELECT fingerprint, t_ms, sqrt(var) AS value FROM rows", wantLabels: dropName},
		{fn: "present_over_time", want: "SELECT fingerprint, t_ms, toFloat64(1) AS value FROM rows", wantLabels: dropName},
		{fn: "last_over_time", want: "SELECT fingerprint, t_ms, last.2 AS value FROM rows", wantLabels: "label_set"},
		{fn: "", want: "SELECT fingerprint, t_ms, last.2 AS value FROM rows WHERE last.1 > stale_at",
			wantLabels: "label_set"},
	} {
		t.Run(tc.fn, func(t *testing.T) {
			sql := PushdownSQL(Pushdown{Grid: probeGrid, Func: tc.fn, RangeMs: 600000, Matchers: probeSelector()})
			points, lbls := pointsOf(t, sql)
			if tc.want != "" && points != tc.want {
				t.Errorf("points\ngot  %s\nwant %s", points, tc.want)
			}
			if tc.wantPrefix != "" && !strings.HasPrefix(points, tc.wantPrefix) {
				t.Errorf("points\ngot  %s\nwant prefix %s", points, tc.wantPrefix)
			}
			if lbls != tc.wantLabels {
				t.Errorf("labels = %s, want %s", lbls, tc.wantLabels)
			}
		})
	}
}

func TestPushdownSQLAggregation(t *testing.T) {
	const perSeries = "SELECT fingerprint, label_set AS labels, t_ms, value FROM (" +
		"SELECT fingerprint, t_ms, last.2 AS value FROM rows WHERE last.1 > stale_at) AS points " +
		"INNER JOIN fp USING (fingerprint)"
	for _, tc := range []struct {
		name string
		agg  Aggregation
		want string
	}{
		{"sum by", Aggregation{Op: "sum", Grouping: []string{"job", "env"}},
			"SELECT cityHash64(grp) AS fingerprint, " +
				"mapFromArrays(arrayMap(x -> x.1, grp), arrayMap(x -> x.2, grp)) AS labels, t_ms, " +
				"if(isFinite(sum(value)), sumKahan(value), sum(value)) AS value " +
				"FROM (SELECT arraySort(arrayFilter(x -> has(['job', 'env'], x.1), " +
				"arrayZip(mapKeys(labels), mapValues(labels)))) AS grp, t_ms, value FROM (" + perSeries + ")) " +
				"GROUP BY grp, t_ms ORDER BY fingerprint, t_ms"},
		{"without drops the name too", Aggregation{Op: "max", Grouping: []string{"instance"}, Without: true},
			"SELECT cityHash64(grp) AS fingerprint, " +
				"mapFromArrays(arrayMap(x -> x.1, grp), arrayMap(x -> x.2, grp)) AS labels, t_ms, " +
				"ifNull(maxIfOrNull(value, NOT isNaN(value)), nan) AS value " +
				"FROM (SELECT arraySort(arrayFilter(x -> NOT has(['instance', '__name__'], x.1), " +
				"arrayZip(mapKeys(labels), mapValues(labels)))) AS grp, t_ms, value FROM (" + perSeries + ")) " +
				"GROUP BY grp, t_ms ORDER BY fingerprint, t_ms"},
		{"avg", Aggregation{Op: "avg"},
			"SELECT cityHash64(grp) AS fingerprint, " +
				"mapFromArrays(arrayMap(x -> x.1, grp), arrayMap(x -> x.2, grp)) AS labels, t_ms, " +
				"if(isFinite(sum(value)), sumKahan(value), sum(value)) / count() AS value " +
				"FROM (SELECT arraySort(arrayFilter(x -> has([], x.1), " +
				"arrayZip(mapKeys(labels), mapValues(labels)))) AS grp, t_ms, value FROM (" + perSeries + ")) " +
				"GROUP BY grp, t_ms ORDER BY fingerprint, t_ms"},
		{"count without grouping", Aggregation{Op: "count"},
			"SELECT cityHash64(grp) AS fingerprint, " +
				"mapFromArrays(arrayMap(x -> x.1, grp), arrayMap(x -> x.2, grp)) AS labels, t_ms, toFloat64(count()) AS value " +
				"FROM (SELECT arraySort(arrayFilter(x -> has([], x.1), " +
				"arrayZip(mapKeys(labels), mapValues(labels)))) AS grp, t_ms, value FROM (" + perSeries + ")) " +
				"GROUP BY grp, t_ms ORDER BY fingerprint, t_ms"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			agg := tc.agg
			sql := PushdownSQL(Pushdown{Grid: probeGrid, RangeMs: 300000, Matchers: probeSelector(), Aggregation: &agg})
			i := strings.Index(sql, "HAVING count > 0) ")
			if got := sql[i+len("HAVING count > 0) "):]; got != tc.want {
				t.Fatalf("got  %s\nwant %s", got, tc.want)
			}
		})
	}
}
