package metricread

import (
	"fmt"

	"github.com/metrico/qryn/v5/reader/utils/tables"
	"github.com/prometheus/prometheus/model/labels"
)

// Grid is a query's evaluation timestamps StartMs + k·StepMs up to EndMs, in unix ms.
// An instant query has StartMs = EndMs and StepMs 0.
type Grid struct {
	StartMs int64
	EndMs   int64
	StepMs  int64
}

// Pushdown is a range function over one selector, or the instant selector itself when Func
// is empty, evaluated at every timestamp of Grid over (t − RangeMs, t].
type Pushdown struct {
	Grid     Grid
	Func     string
	RangeMs  int64
	Matchers []*labels.Matcher
}

// PushdownSQL evaluates p from raw samples. Rows: fingerprint UInt64,
// labels Map(String, String), t_ms Int64, value Float64, ordered by fingerprint and t_ms.
func PushdownSQL(p Pushdown) string {
	window := Window{FromMs: p.Grid.StartMs - p.RangeMs, ToMs: p.Grid.EndMs}
	return fmt.Sprintf("WITH fp AS (%s), rows AS (%s) "+
		"SELECT fingerprint, %s AS labels, t_ms, value FROM (%s) AS points "+
		"INNER JOIN fp USING (fingerprint) "+
		"ORDER BY fingerprint, t_ms",
		SeriesSQL(window, p.Matchers), rawRowsSQL(p), outputLabels(p), valueSQL(p))
}

// rawRowsSQL is the row shape per (fingerprint, t) from raw samples: each sample joined to
// the steps whose window holds it, its predecessor the previous non-stale sample.
func rawRowsSQL(p Pushdown) string {
	return fmt.Sprintf("WITH %d AS start_ms, %d AS end_ms, %d AS step_ms, %d AS range_ms, "+
		"intDiv(end_ms - start_ms, step_ms) + 1 AS n_steps, "+
		"NOT stale AND prev_ms > start_ms + k * step_ms - range_ms AS paired "+
		"SELECT fingerprint, start_ms + k * step_ms AS t_ms, "+
		"minIf((timestamp, value), NOT stale) AS first, "+
		"maxIf((timestamp, value), NOT stale) AS last, "+
		"countIf(NOT stale) AS count, "+
		"sumIf(value, NOT stale) AS sum, "+
		"minIf(value, NOT stale) AS min, "+
		"maxIf(value, NOT stale) AS max, "+
		"countIf(paired AND value < prev_value) AS resets, "+
		"sumIf(prev_value, paired AND value < prev_value) AS reset_drop, "+
		"countIf(paired AND value != prev_value AND NOT (isNaN(value) AND isNaN(prev_value))) AS changes, "+
		"maxIf(timestamp, stale) AS stale_at, "+
		"argMaxIf(prev, timestamp, NOT stale) AS penult, "+
		"varPopStableIf(value, NOT stale) AS var "+
		"FROM (SELECT fingerprint, timestamp, value, "+
		"toUnixTimestamp64Milli(timestamp) AS ts_ms, "+
		"reinterpretAsUInt64(value) = 0x7ff0000000000002 AS stale, "+
		"maxIf((timestamp, value), NOT stale) OVER (PARTITION BY fingerprint ORDER BY timestamp "+
		"ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev, "+
		"toUnixTimestamp64Milli(prev.1) AS prev_ms, prev.2 AS prev_value, "+
		"greatest(0, if(ts_ms >= start_ms, intDiv(ts_ms - start_ms + step_ms - 1, step_ms), "+
		"-intDiv(start_ms - ts_ms, step_ms))) AS k_min, "+
		"least(n_steps - 1, intDiv(ts_ms + range_ms - 1 - start_ms, step_ms)) AS k_max "+
		"FROM (SELECT fingerprint, timestamp, value FROM %s "+
		"WHERE fingerprint IN (SELECT fingerprint FROM fp) "+
		"AND timestamp > fromUnixTimestamp64Milli(start_ms - range_ms) "+
		"AND timestamp <= fromUnixTimestamp64Milli(end_ms) "+
		"ORDER BY fingerprint, timestamp "+
		"LIMIT 1 BY fingerprint, timestamp)) "+
		"ARRAY JOIN range(k_min, k_max + 1) AS k "+
		"GROUP BY fingerprint, k "+
		"HAVING count > 0",
		p.Grid.StartMs, p.Grid.EndMs, max(p.Grid.StepMs, 1), p.RangeMs, tables.GetTableName("metric_samples"))
}

// valueSQL turns the row shape into the function's value at each t, as Prometheus computes it.
// Rows: fingerprint, t_ms, value.
func valueSQL(p Pushdown) string {
	return functions[p.Func](p.RangeMs)
}

// functions holds the value of each pushed-down function over the row shape; "" is the
// instant selector, the last sample of (t − lookback, t] unless a stale marker follows it.
var functions = map[string]func(rangeMs int64) string{
	"":                  points("last.2", "last.1 > stale_at"),
	"rate":              func(r int64) string { return extrapolatedSQL(r, true, true) },
	"increase":          func(r int64) string { return extrapolatedSQL(r, true, false) },
	"delta":             func(r int64) string { return extrapolatedSQL(r, false, false) },
	"irate":             points("if(last.2 < penult.2, last.2, last.2 - penult.2) / ((toUnixTimestamp64Milli(last.1) - toUnixTimestamp64Milli(penult.1)) / 1000)", "count >= 2"),
	"idelta":            points("last.2 - penult.2", "count >= 2"),
	"resets":            points("toFloat64(resets)", ""),
	"changes":           points("toFloat64(changes)", ""),
	"count_over_time":   points("toFloat64(count)", ""),
	"sum_over_time":     points("sum", ""),
	"min_over_time":     points("min", ""),
	"max_over_time":     points("max", ""),
	"avg_over_time":     points("sum / count", ""),
	"stdvar_over_time":  points("var", ""),
	"stddev_over_time":  points("sqrt(var)", ""),
	"present_over_time": points("toFloat64(1)", ""),
	"last_over_time":    points("last.2", ""),
}

// Pushable reports whether the range function fn is evaluated by PushdownSQL.
func Pushable(fn string) bool {
	_, ok := functions[fn]
	return fn != "" && ok
}

func points(value, where string) func(int64) string {
	sql := "SELECT fingerprint, t_ms, " + value + " AS value FROM rows"
	if where != "" {
		sql += " WHERE " + where
	}
	return func(int64) string { return sql }
}

// extrapolatedSQL follows promql/functions.go extrapolatedRate: a point needs two samples, the
// change is extrapolated to each window edge by up to half the average gap, and a counter's
// no further back than it would reach zero.
func extrapolatedSQL(rangeMs int64, counter, perSecond bool) string {
	return fmt.Sprintf("WITH %d AS range_ms, %d AS is_counter, %d AS per_second, "+
		"t_ms - range_ms AS window_start_ms, "+
		"toUnixTimestamp64Milli(first.1) AS first_ms, toUnixTimestamp64Milli(last.1) AS last_ms, "+
		"(last_ms - first_ms) / 1000 AS sampled_s, "+
		"sampled_s / (count - 1) AS avg_gap_s, "+
		"(first_ms - window_start_ms) / 1000 AS to_start_s, "+
		"(t_ms - last_ms) / 1000 AS to_end_s, "+
		"if(is_counter, last.2 - first.2 + reset_drop, last.2 - first.2) AS change, "+
		"if(to_start_s >= avg_gap_s * 1.1, avg_gap_s / 2, to_start_s) AS ext_start_s, "+
		"if(is_counter AND change > 0 AND first.2 >= 0 AND sampled_s * (first.2 / change) < ext_start_s, "+
		"sampled_s * (first.2 / change), ext_start_s) AS ext_start_capped_s, "+
		"if(to_end_s >= avg_gap_s * 1.1, avg_gap_s / 2, to_end_s) AS ext_end_s, "+
		"(sampled_s + ext_start_capped_s + ext_end_s) / sampled_s AS factor "+
		"SELECT fingerprint, t_ms, change * (factor / if(per_second, range_ms / 1000, 1)) AS value "+
		"FROM rows WHERE count >= 2",
		rangeMs, flag(counter), flag(perSecond))
}

// outputLabels is the label set Prometheus returns for p's function.
func outputLabels(p Pushdown) string {
	if p.Func == "" || p.Func == "last_over_time" {
		return "label_set"
	}
	return "mapFilter((k, v) -> k != '__name__', label_set)"
}

func flag(b bool) int {
	if b {
		return 1
	}
	return 0
}
