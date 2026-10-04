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
	want := "WITH rows AS (WITH 1767225600000 AS start_ms, 1767226200000 AS end_ms, 60000 AS step_ms, 300000 AS range_ms, " +
		"intDiv(end_ms - start_ms, step_ms) + 1 AS n_steps, " +
		"NOT stale AND prev_ms > start_ms + k * step_ms - range_ms AS paired " +
		"SELECT fingerprint, start_ms + k * step_ms AS t_ms, " +
		"(minIf(timestamp, NOT stale), argMinIf(value, timestamp, NOT stale)) AS first, " +
		"(maxIf(timestamp, NOT stale), argMaxIf(value, timestamp, NOT stale)) AS last, " +
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
		"FROM (SELECT fingerprint, timestamp, value FROM metric_samples FINAL " +
		"WHERE " + localSeries(1767223500000, 1767226200000) + " " +
		"AND timestamp > fromUnixTimestamp64Milli(start_ms - range_ms) " +
		"AND timestamp <= fromUnixTimestamp64Milli(end_ms))) " +
		"ARRAY JOIN range(k_min, k_max + 1) AS k " +
		"GROUP BY fingerprint, k " +
		"HAVING count > 0) " +
		"SELECT fingerprint, t_ms, value FROM (" +
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
		"FROM rows WHERE count >= 2) " +
		"ORDER BY fingerprint, t_ms " +
		"SETTINGS do_not_merge_across_partitions_select_final = 1"
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

// pointsOf returns the value select of a pushdown's SQL.
func pointsOf(t *testing.T, sql string) string {
	t.Helper()
	const head, tail = ") SELECT fingerprint, t_ms, value FROM (", ") ORDER BY fingerprint, t_ms"
	i := strings.LastIndex(sql, head)
	j := strings.LastIndex(sql, tail)
	if i < 0 || j < i {
		t.Fatalf("no value select in %s", sql)
	}
	return sql[i+len(head) : j]
}

func TestPushdownSQLFunctionValues(t *testing.T) {
	extrapolated := func(counter, perSecond int) string {
		return fmt.Sprintf("WITH 600000 AS range_ms, %d AS is_counter, %d AS per_second, ", counter, perSecond)
	}
	for _, tc := range []struct {
		fn         string
		wantPrefix string
		want       string
	}{
		{fn: "increase", wantPrefix: extrapolated(1, 0)},
		{fn: "delta", wantPrefix: extrapolated(0, 0)},
		{fn: "irate", want: "SELECT fingerprint, t_ms, if(last.2 < penult.2, last.2, last.2 - penult.2) / " +
			"((toUnixTimestamp64Milli(last.1) - toUnixTimestamp64Milli(penult.1)) / 1000) AS value " +
			"FROM rows WHERE count >= 2"},
		{fn: "idelta", want: "SELECT fingerprint, t_ms, last.2 - penult.2 AS value FROM rows WHERE count >= 2"},
		{fn: "resets", want: "SELECT fingerprint, t_ms, toFloat64(resets) AS value FROM rows"},
		{fn: "changes", want: "SELECT fingerprint, t_ms, toFloat64(changes) AS value FROM rows"},
		{fn: "count_over_time", want: "SELECT fingerprint, t_ms, toFloat64(count) AS value FROM rows"},
		{fn: "sum_over_time", want: "SELECT fingerprint, t_ms, sum AS value FROM rows"},
		{fn: "min_over_time", want: "SELECT fingerprint, t_ms, min AS value FROM rows"},
		{fn: "max_over_time", want: "SELECT fingerprint, t_ms, max AS value FROM rows"},
		{fn: "avg_over_time", want: "SELECT fingerprint, t_ms, sum / count AS value FROM rows"},
		{fn: "stdvar_over_time", want: "SELECT fingerprint, t_ms, var AS value FROM rows"},
		{fn: "stddev_over_time", want: "SELECT fingerprint, t_ms, sqrt(var) AS value FROM rows"},
		{fn: "present_over_time", want: "SELECT fingerprint, t_ms, toFloat64(1) AS value FROM rows"},
		{fn: "last_over_time", want: "SELECT fingerprint, t_ms, last.2 AS value FROM rows"},
		{fn: "", want: "SELECT fingerprint, t_ms, last.2 AS value FROM rows WHERE last.1 > stale_at"},
	} {
		t.Run(tc.fn, func(t *testing.T) {
			points := pointsOf(t, PushdownSQL(Pushdown{Grid: probeGrid, Func: tc.fn, RangeMs: 600000, Matchers: probeSelector()}))
			if tc.want != "" && points != tc.want {
				t.Errorf("points\ngot  %s\nwant %s", points, tc.want)
			}
			if tc.wantPrefix != "" && !strings.HasPrefix(points, tc.wantPrefix) {
				t.Errorf("points\ngot  %s\nwant prefix %s", points, tc.wantPrefix)
			}
		})
	}
}

// probeSeries is the series selection of the probe selector over (start − 600000, end].
const probeSeries = "SELECT fingerprint, any(labels) AS label_set FROM metric_series " +
	"WHERE (name = 'x') " +
	"AND last_seen >= fromUnixTimestamp64Milli(1767223200000) " +
	"AND first_seen <= fromUnixTimestamp64Milli(1767226200000) " +
	"GROUP BY fingerprint"

func TestPushdownLabelsSQLNamesEachSeriesOnce(t *testing.T) {
	const dropName = "SELECT fingerprint, mapFilter((k, v) -> k != '__name__', label_set) AS labels FROM (" +
		probeSeries + ")"
	const keepName = "SELECT fingerprint, label_set AS labels FROM (" + probeSeries + ")"
	for fn, want := range map[string]string{
		"rate": dropName, "increase": dropName, "irate": dropName, "sum_over_time": dropName,
		"present_over_time": dropName, "last_over_time": keepName, "": keepName,
	} {
		if got := PushdownLabelsSQL(Pushdown{Grid: probeGrid, Func: fn, RangeMs: 600000, Matchers: probeSelector()}); got != want {
			t.Errorf("%q:\ngot  %s\nwant %s", fn, got, want)
		}
	}
}

func TestPushdownLabelsSQLNamesEachGroupOnce(t *testing.T) {
	for _, tc := range []struct {
		name string
		agg  Aggregation
		keep string
	}{
		{"by", Aggregation{Op: "sum", Grouping: []string{"job", "env"}}, "has(['job', 'env'], x.1)"},
		{"without drops the name too", Aggregation{Op: "max", Grouping: []string{"instance"}, Without: true},
			"NOT has(['instance', '__name__'], x.1)"},
		{"no grouping", Aggregation{Op: "count"}, "has([], x.1)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			agg := tc.agg
			got := PushdownLabelsSQL(Pushdown{Grid: probeGrid, RangeMs: 600000, Matchers: probeSelector(), Aggregation: &agg})
			want := "SELECT cityHash64(grp) AS fingerprint, " +
				"mapFromArrays(arrayMap(x -> x.1, grp), arrayMap(x -> x.2, grp)) AS labels " +
				"FROM (SELECT DISTINCT arraySort(arrayFilter(x -> " + tc.keep + ", " +
				"arrayZip(mapKeys(label_set), mapValues(label_set)))) AS grp FROM (" + probeSeries + "))"
			if got != want {
				t.Errorf("got  %s\nwant %s", got, want)
			}
		})
	}
}

func TestPushdownLabelsSQLSelectsTheSeriesTheTierReads(t *testing.T) {
	// increase[1m] at 1m steps from the 5m tier reads (00:00 − 5m, 00:10] after snapping.
	got := PushdownLabelsSQL(Pushdown{Grid: Grid{StartMs: 1767225660000, EndMs: 1767226260000, StepMs: 60000},
		Func: "increase", RangeMs: 60000, Matchers: probeSelector(), Tier: Tier5m})
	if !strings.Contains(got, "last_seen >= fromUnixTimestamp64Milli(1767223500000) "+
		"AND first_seen <= fromUnixTimestamp64Milli(1767226200000) ") {
		t.Errorf("labels not read over the tier's window: %s", got)
	}
}

func TestPushdownSQLCarriesNoLabelsPerPoint(t *testing.T) {
	agg := Aggregation{Op: "sum", Grouping: []string{"job"}}
	for name, p := range map[string]Pushdown{
		"raw":         {Grid: probeGrid, Func: "rate", RangeMs: 300000, Matchers: probeSelector()},
		"raw instant": {Grid: probeGrid, RangeMs: 300000, Matchers: probeSelector()},
		"tier":        {Grid: probeGrid, Func: "rate", RangeMs: 300000, Matchers: probeSelector(), Tier: Tier5m},
		"stamped":     {Grid: Grid{StartMs: 1767225660000, EndMs: 1767226260000, StepMs: 60000}, Func: "rate", RangeMs: 300000, Matchers: probeSelector(), Tier: Tier5m},
		"aggregated":  {Grid: probeGrid, Func: "rate", RangeMs: 300000, Matchers: probeSelector(), Aggregation: &agg},
	} {
		sql := PushdownSQL(p)
		if strings.Contains(sql, "label_set") && p.Aggregation == nil {
			t.Errorf("%s reads labels: %s", name, sql)
		}
		if p.Aggregation != nil && strings.Count(sql, "mapKeys(label_set)") != 1 {
			t.Errorf("%s builds the group key other than once per series: %s", name, sql)
		}
	}
}

func TestRawPushdownDedupsWithFinalPerPartition(t *testing.T) {
	agg := Aggregation{Op: "sum", Grouping: []string{"job"}}
	for name, p := range map[string]Pushdown{
		"rate":       {Grid: probeGrid, Func: "rate", RangeMs: 300000, Matchers: probeSelector()},
		"instant":    {Grid: Grid{StartMs: probeGrid.EndMs, EndMs: probeGrid.EndMs}, RangeMs: 300000, Matchers: probeSelector()},
		"aggregated": {Grid: probeGrid, Func: "increase", RangeMs: 300000, Matchers: probeSelector(), Aggregation: &agg},
		"cluster":    {Grid: probeGrid, Func: "rate", RangeMs: 300000, Matchers: probeSelector(), Cluster: true},
	} {
		sql := PushdownSQL(p)
		table := "metric_samples"
		if p.Cluster {
			table += "_dist"
		}
		if !strings.Contains(sql, " FROM "+table+" FINAL WHERE ") || strings.Contains(sql, "LIMIT 1 BY") {
			t.Errorf("%s does not deduplicate with FINAL: %s", name, sql)
		}
		if !strings.HasSuffix(sql, " SETTINGS do_not_merge_across_partitions_select_final = 1") {
			t.Errorf("%s merges across partitions: %s", name, sql)
		}
	}
}

func TestPushdownSQLFirstAndLastAreScalarStates(t *testing.T) {
	rows := rowsOf(t, PushdownSQL(Pushdown{Grid: probeGrid, Func: "increase", RangeMs: 300000, Matchers: probeSelector()}))
	for _, want := range []string{
		"(minIf(timestamp, NOT stale), argMinIf(value, timestamp, NOT stale)) AS first",
		"(maxIf(timestamp, NOT stale), argMaxIf(value, timestamp, NOT stale)) AS last",
		"maxIf(timestamp, stale) AS stale_at",
	} {
		if !strings.Contains(rows, want) {
			t.Errorf("rows lack %s: %s", want, rows)
		}
	}
	if strings.Contains(rows, "minIf((timestamp, value)") || strings.Contains(rows, "AS first, maxIf((timestamp, value)") {
		t.Errorf("rows keep a tuple state for first or last: %s", rows)
	}
}

func TestPushdownSQLAggregation(t *testing.T) {
	const perSeries = "SELECT fingerprint, t_ms, value FROM (" +
		"SELECT fingerprint, t_ms, last.2 AS value FROM rows WHERE last.1 > stale_at)"
	groups := func(keep string) string {
		return "WITH fp AS (SELECT fingerprint, cityHash64(arraySort(arrayFilter(x -> " + keep + ", " +
			"arrayZip(mapKeys(label_set), mapValues(label_set))))) AS group_fp FROM (" +
			"SELECT fingerprint, any(labels) AS label_set FROM metric_series " +
			"WHERE (name = 'x') " +
			"AND last_seen >= fromUnixTimestamp64Milli(1767223500000) " +
			"AND first_seen <= fromUnixTimestamp64Milli(1767226200000) " +
			"GROUP BY fingerprint)), rows AS ("
	}
	for _, tc := range []struct {
		name       string
		agg        Aggregation
		wantGroups string
		want       string
	}{
		{"sum by", Aggregation{Op: "sum", Grouping: []string{"job", "env"}}, groups("has(['job', 'env'], x.1)"),
			"SELECT group_fp AS fingerprint, t_ms, if(isFinite(sum(value)), sumKahan(value), sum(value)) AS value " +
				"FROM (SELECT group_fp, t_ms, value FROM (" + perSeries + ") AS points INNER JOIN fp USING (fingerprint)) " +
				"GROUP BY group_fp, t_ms ORDER BY fingerprint, t_ms"},
		{"without drops the name too", Aggregation{Op: "max", Grouping: []string{"instance"}, Without: true},
			groups("NOT has(['instance', '__name__'], x.1)"),
			"SELECT group_fp AS fingerprint, t_ms, ifNull(maxIfOrNull(value, NOT isNaN(value)), nan) AS value " +
				"FROM (SELECT group_fp, t_ms, value FROM (" + perSeries + ") AS points INNER JOIN fp USING (fingerprint)) " +
				"GROUP BY group_fp, t_ms ORDER BY fingerprint, t_ms"},
		{"avg", Aggregation{Op: "avg"}, groups("has([], x.1)"),
			"SELECT group_fp AS fingerprint, t_ms, if(isFinite(sum(value)), sumKahan(value), sum(value)) / count() AS value " +
				"FROM (SELECT group_fp, t_ms, value FROM (" + perSeries + ") AS points INNER JOIN fp USING (fingerprint)) " +
				"GROUP BY group_fp, t_ms ORDER BY fingerprint, t_ms"},
		{"count without grouping", Aggregation{Op: "count"}, groups("has([], x.1)"),
			"SELECT group_fp AS fingerprint, t_ms, toFloat64(count()) AS value " +
				"FROM (SELECT group_fp, t_ms, value FROM (" + perSeries + ") AS points INNER JOIN fp USING (fingerprint)) " +
				"GROUP BY group_fp, t_ms ORDER BY fingerprint, t_ms"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			agg := tc.agg
			sql := PushdownSQL(Pushdown{Grid: probeGrid, RangeMs: 300000, Matchers: probeSelector(), Aggregation: &agg})
			if !strings.HasPrefix(sql, tc.wantGroups) {
				t.Errorf("groups\ngot  %s\nwant prefix %s", sql, tc.wantGroups)
			}
			const settings = " SETTINGS do_not_merge_across_partitions_select_final = 1"
			i := strings.Index(sql, "HAVING count > 0) ")
			if got := strings.TrimSuffix(sql[i+len("HAVING count > 0) "):], settings); got != tc.want {
				t.Fatalf("got  %s\nwant %s", got, tc.want)
			}
		})
	}
}
