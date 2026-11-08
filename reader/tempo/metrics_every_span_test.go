package tempo

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	ct "github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/stretchr/testify/require"
)

func everySpanContext(cluster, skip bool) *shared.PlannerContext {
	ctx := &shared.PlannerContext{
		IsCluster: cluster, SkipUnavailableShards: skip,
		ClusterName: "c", DistSuffix: "_dist",
		From: time.Unix(86390, 0).UTC(), To: time.Unix(86410, 0).UTC(),
		TracesTable: "tempo_traces", TracesDistTable: "tempo_traces_dist",
		TracesAttrsTable: "tempo_traces_attrs_gin", TracesAttrsDistTable: "tempo_traces_attrs_gin_dist",
	}
	if !cluster {
		ctx.TracesDistTable = ctx.TracesTable
		ctx.TracesAttrsDistTable = ctx.TracesAttrsTable
	}
	return ctx
}

// everySpanStatements are the statements of query under plan: series, instant, exemplars.
type everySpanStatements struct {
	series, instant, exemplars string
}

func renderEverySpan(t *testing.T, query string, cluster, skip bool) everySpanStatements {
	t.Helper()
	ctx := everySpanContext(cluster, skip)
	q, err := traceql_ir.Parse(query)
	require.NoError(t, err, query)
	attrs := ctx.TracesAttrsTable
	if cluster {
		attrs = ctx.TracesAttrsDistTable
	}
	req := MetricsRequest{Fn: q.Metrics.Fn, Attr: q.Metrics.Attr, By: q.Metrics.By, Quantiles: q.Metrics.Quantiles,
		FromNS: ctx.From.UnixNano(), ToNS: ctx.To.UnixNano(), StepNS: 30e9, AttrsTable: attrs,
		Scale: ct.SampleScale(q)}
	plan, err := ct.PlanMetrics(q, func(spans *sql.With) (sql.ISelect, error) {
		r := req
		r.Spans = spans
		var sel sql.ISelect
		var err error
		if req.Fn == "histogram_over_time" {
			sel, err = BuildHistogramRangeQuery(&r)
		} else {
			sel, _, err = BuildMetricsRangeQuery(&r)
		}
		return sel, err
	})
	require.NoError(t, err, query)
	draw := func(p shared.SQLRequestPlanner) string {
		stmt, err := p.Process(ctx)
		require.NoError(t, err, query)
		return render(t, stmt)
	}
	var labels []string
	var res everySpanStatements
	if req.Fn == "histogram_over_time" {
		series, err := HistogramRange(plan, req)
		require.NoError(t, err, query)
		res.series = draw(series)
		ex, err := HistogramExemplars(plan, req)
		require.NoError(t, err, query)
		res.exemplars = draw(ex)
		return res
	}
	series, err := RangeSeries(plan, req, &labels)
	require.NoError(t, err, query)
	res.series = draw(series)
	instant, err := InstantSeries(plan, req, &labels)
	require.NoError(t, err, query)
	res.instant = draw(instant)
	ex, err := GenericExemplars(plan, req, &labels)
	require.NoError(t, err, query)
	res.exemplars = draw(ex)
	return res
}

var (
	countOnShards = regexp.MustCompile(`shard_rows as \( SELECT \* FROM cluster\('c', view\(.*FROM tempo_traces as main.*\)\)`)
	groupedByPair = regexp.MustCompile(`GROUP BY traces\.trace_id, traces\.span_id`)
)

// requireReadsTheRows checks that stmt reads the traces rows as stored, with no
// span hash and no setting.
func requireReadsTheRows(t *testing.T, stmt, table, query string) {
	t.Helper()
	require.Contains(t, stmt, "FROM "+table+" as main", query)
	require.NotContains(t, stmt, "cityHash64(main.trace_id", query)
	require.NotContains(t, stmt, "uniqExact", query)
	require.NotContains(t, stmt, "shard_rows", query)
	require.NotContains(t, stmt, "metrics_spans", query)
	require.NotContains(t, stmt, "cluster(", query)
	require.NotContains(t, stmt, "SETTINGS", query)
	require.NotRegexp(t, groupedByPair, stmt, query)
}

// A lone `{}` count counts the rows as stored, per bucket.
func TestEverySpanCountsReadTheRows(t *testing.T) {
	for _, tc := range []struct{ query, value, group string }{
		{`{} | rate()`, `toFloat64(count(*)) / 30.000000 as value`, `GROUP BY ts ORDER BY ts`},
		{`({}) | rate()`, `toFloat64(count(*)) / 30.000000 as value`, `GROUP BY ts ORDER BY ts`},
		{`{} | select(.b) | rate()`, `toFloat64(count(*)) / 30.000000 as value`, `GROUP BY ts ORDER BY ts`},
		{`{} | count_over_time()`, `count(*) as value`, `GROUP BY ts ORDER BY ts`},
		{`{} | count_over_time() with(sample=0.5)`, `(count(*)) * 2 as value`, `GROUP BY ts ORDER BY ts`},
		{`{} | rate() by (name)`, `toFloat64(count(*)) / 30.000000 as value, main.name as by_0`, `GROUP BY ts, by_0 ORDER BY ts`},
		{`{} | histogram_over_time(duration)`, `as bucket_ns, count(*) as cnt`, `GROUP BY ts, bucket_ns ORDER BY ts`},
	} {
		for _, skip := range []bool{false, true} {
			got := renderEverySpan(t, tc.query, true, skip).series
			requireReadsTheRows(t, got, "tempo_traces_dist", tc.query)
			require.Contains(t, got, tc.value, tc.query)
			require.Contains(t, got, tc.group, tc.query)
		}
		single := renderEverySpan(t, tc.query, false, false).series
		requireReadsTheRows(t, single, "tempo_traces", tc.query)
		require.Contains(t, single, tc.value, tc.query)
	}
}

// The window is an instant count's one bucket.
func TestEverySpanInstantCountHasNoBucket(t *testing.T) {
	got := renderEverySpan(t, `{} | rate() by (name)`, true, false).instant
	requireReadsTheRows(t, got, "tempo_traces_dist", "instant rate by name")
	require.Contains(t, got, "toFloat64(count(*)) / 20.000000 as value, main.name as by_0")
	require.NotContains(t, got, "intDiv")
	require.True(t, strings.HasSuffix(got, "GROUP BY by_0"), got)

	got = renderEverySpan(t, `{} | count_over_time()`, true, true).instant
	requireReadsTheRows(t, got, "tempo_traces_dist", "instant count")
	require.Contains(t, got, "SELECT count(*) as value FROM")
	require.NotContains(t, got, "GROUP BY")
}

// Skipping unavailable shards sets the setting in the statement that reads the shards.
func TestEverySpanCountSkipsInsideTheShardRead(t *testing.T) {
	got := renderEverySpan(t, `{} | rate() by (.a)`, true, true).series
	require.Regexp(t, `\)\)\s+SETTINGS skip_unavailable_shards\s*=\s*.?1.?\s*\) SELECT`, got)
}

// Any other pipeline, a nested-set field and compare() keep the grouped read.
func TestSelectorsKeepTheGroupedRead(t *testing.T) {
	for _, query := range []string{
		`{.a = "1"} | rate()`,
		`{} | {.a = "1"} | rate()`,
		`{true} | rate()`,
		`{} | rate() by (nestedSetLeft)`,
		`{} | avg_over_time(nestedSetRight)`,
	} {
		got := renderEverySpan(t, query, true, false).series
		require.NotContains(t, got, "count() as n", query)
		require.Contains(t, got, "metrics_spans", query)
	}
}

// A lone `{}` takes exemplars straight from the rows, with no setting.
func TestEverySpanExemplarsReadTheRows(t *testing.T) {
	for _, query := range []string{`{} | rate()`, `{} | rate() by (name)`, `{} | quantile_over_time(duration, .5)`,
		`{} | histogram_over_time(duration)`} {
		got := renderEverySpan(t, query, true, true).exemplars
		require.Contains(t, got, "FROM tempo_traces_dist as main", query)
		require.Contains(t, got, "any((lower(hex(main.trace_id))", query)
		require.NotContains(t, got, "metrics_spans", query)
		require.NotContains(t, got, "SETTINGS", query)

		single := renderEverySpan(t, query, false, false).exemplars
		require.Contains(t, single, "FROM tempo_traces as main", query)
	}
}

// Value functions read the rows as stored, an instant quantile included.
func TestEverySpanValuesReadTheRows(t *testing.T) {
	for _, tc := range []struct{ query, value string }{
		{`{} | min_over_time(duration)`, `min(main.duration_ns) / 1e9 as value`},
		{`{} | max_over_time(duration) by (name)`, `max(main.duration_ns) / 1e9 as value, main.name as by_0`},
		{`{} | avg_over_time(duration)`, `avg(main.duration_ns) / 1e9 as value`},
		{`{} | sum_over_time(duration)`, `sum(main.duration_ns) / 1e9 as value`},
		{`{} | avg_over_time(duration) by (name, resource.service.name)`, `avg(main.duration_ns) / 1e9 as value, main.name as by_0, main.service_name as by_1`},
	} {
		for _, skip := range []bool{false, true} {
			got := renderEverySpan(t, tc.query, true, skip)
			for _, stmt := range []string{got.series, got.instant} {
				requireReadsTheRows(t, stmt, "tempo_traces_dist", tc.query)
				require.Contains(t, stmt, tc.value, tc.query)
			}
		}
		single := renderEverySpan(t, tc.query, false, false)
		requireReadsTheRows(t, single.series, "tempo_traces", tc.query)
		requireReadsTheRows(t, single.instant, "tempo_traces", tc.query)
	}
	for _, skip := range []bool{false, true} {
		got := renderEverySpan(t, `{} | quantile_over_time(duration, .5, .9) by (name)`, true, skip).instant
		requireReadsTheRows(t, got, "tempo_traces_dist", "instant quantile")
		require.Contains(t, got, "arrayMap(x -> x / 1e9, quantiles(0.5, 0.9)(main.duration_ns)) as value, main.name as by_0")
		require.True(t, strings.HasSuffix(got, "GROUP BY by_0"), got)
	}
}

// A range quantile of one level reads the rows per bucket, with no query-wide row.
func TestEverySpanRangeQuantileReadsTheRows(t *testing.T) {
	for _, skip := range []bool{false, true} {
		got := renderEverySpan(t, `{} | quantile_over_time(duration, .5) by (name)`, true, skip).series
		requireReadsTheRows(t, got, "tempo_traces_dist", "range quantile")
		require.Contains(t, got, "arrayMap(x -> x / 1e9, quantiles(0.5)(main.duration_ns)) as value, main.name as by_0")
		require.Contains(t, got, "GROUP BY ts, by_0 ORDER BY ts")
		require.NotContains(t, got, "query_wide")
		require.NotContains(t, got, "GROUPING SETS")
	}
	single := renderEverySpan(t, `{} | quantile_over_time(duration, .5)`, false, false).series
	requireReadsTheRows(t, single, "tempo_traces", "range quantile")
}

// A range quantile of several levels takes one state per bucket from each
// shard's rows and merges them on the coordinator, query-wide row included.
func TestEverySpanRangeQuantilesMergeShardStates(t *testing.T) {
	got := renderEverySpan(t, `{} | quantile_over_time(duration, .5, .9) by (name)`, true, false).series
	require.Regexp(t, `shard_rows as \( SELECT \* FROM cluster\('c', view\( SELECT .*quantilesState\(0\.5, 0\.9\)\(main\.duration_ns\) as q_state.* FROM tempo_traces as main WHERE .* GROUP BY ts, by_0\)\)\)`, got)
	require.Contains(t, got, "arrayMap(x -> x / 1e9, quantilesMerge(0.5, 0.9)(q_state)) as value, grouping(ts) as query_wide, by_0 FROM shard_rows")
	require.Contains(t, got, "GROUP BY GROUPING SETS ((ts, by_0), ()) ORDER BY ts")
	require.NotContains(t, got, "cityHash64")
	require.NotContains(t, got, "metrics_spans")
	require.NotContains(t, got, "SETTINGS")

	skip := renderEverySpan(t, `{} | quantile_over_time(duration, .5, .9)`, true, true).series
	require.Regexp(t, `\)\)\s+SETTINGS skip_unavailable_shards\s*=\s*.?1.?\s*\) SELECT`, skip)

	single := renderEverySpan(t, `{} | quantile_over_time(duration, .5, .9)`, false, false).series
	require.Contains(t, single, "quantilesMerge(0.5, 0.9)(q_state)")
	require.Contains(t, single, "FROM tempo_traces as main")
	require.NotContains(t, single, "cluster(")
	require.NotContains(t, single, "cityHash64")
}

// A custom by-label pivots on each shard over its local attributes.
func TestEverySpanCustomByStaysOnTheShard(t *testing.T) {
	got := renderEverySpan(t, `{} | rate() by (.a, name) with(sample=0.5)`, true, false)
	require.Regexp(t, countOnShards, got.series)
	require.Contains(t, got.series, "count() as n")
	require.NotContains(t, got.series, "cityHash64(main.trace_id")
	require.Contains(t, got.series, "FROM tempo_traces_attrs_gin FINAL")
	require.NotContains(t, got.series, "_dist")
	require.Contains(t, got.series, "cityHash64(main.span_id) <")
	require.Contains(t, got.series, "toFloat64(sum(n)) / 15 as value, by_1, by_custom_0")

	require.Contains(t, got.exemplars, "FROM tempo_traces_attrs_gin FINAL")
	require.NotContains(t, got.exemplars, "_dist")
	require.Contains(t, got.exemplars, "ts, trace_id, dur_ns, span_ts, arg_value, by_1, by_custom_0 FROM shard_rows")
	require.Contains(t, got.exemplars, "LIMIT 1 BY ts, by_1, by_custom_0")
	require.NotContains(t, got.exemplars, "SETTINGS")
}

// A value function under a custom by-label reads the rows as stored and pivots
// the attributes once on the coordinator, whatever the skip setting.
func TestEverySpanValuesReadTheRowsUnderCustomBy(t *testing.T) {
	for _, tc := range []struct{ query, value string }{
		{`{} | avg_over_time(duration) by (.a)`, `avg(main.duration_ns) / 1e9 as value, by_attrs.by_lbl_0 as by_custom_0`},
		{`{} | sum_over_time(duration) by (.a, name)`, `sum(main.duration_ns) / 1e9 as value, main.name as by_1, by_attrs.by_lbl_0 as by_custom_0`},
		{`{} | min_over_time(duration) by (.a)`, `min(main.duration_ns) / 1e9 as value`},
		{`{} | max_over_time(duration) by (.a)`, `max(main.duration_ns) / 1e9 as value`},
		{`{} | quantile_over_time(duration, .5) by (.a)`, `quantiles(0.5)(main.duration_ns)`},
	} {
		for _, skip := range []bool{false, true} {
			got := renderEverySpan(t, tc.query, true, skip)
			for _, stmt := range []string{got.series, got.instant} {
				requireReadsTheRows(t, stmt, "tempo_traces_dist", tc.query)
				require.Contains(t, stmt, tc.value, tc.query)
				require.Contains(t, stmt, "FROM tempo_traces_attrs_gin_dist FINAL", tc.query)
				require.Contains(t, stmt, "GLOBAL LEFT JOIN by_attrs as by_attrs ON", tc.query)
			}
		}
		single := renderEverySpan(t, tc.query, false, false)
		for _, stmt := range []string{single.series, single.instant} {
			requireReadsTheRows(t, stmt, "tempo_traces", tc.query)
			require.Contains(t, stmt, "FROM tempo_traces_attrs_gin FINAL", tc.query)
			require.Contains(t, stmt, " LEFT JOIN by_attrs as by_attrs ON", tc.query)
			require.NotContains(t, stmt, "GLOBAL", tc.query)
		}
	}
}

// A quantile list under a custom by-label merges shard states over each
// shard's attributes in range, and reads the rows when instant.
func TestEverySpanQuantileListUnderCustomBy(t *testing.T) {
	const query = `{} | quantile_over_time(duration, .5, .9) by (.a)`
	got := renderEverySpan(t, query, true, false)
	require.Regexp(t, `shard_rows as \( SELECT \* FROM cluster\('c', view\(.*quantilesState\(0\.5, 0\.9\)\(main\.duration_ns\) as q_state.*FROM tempo_traces as main`, got.series)
	require.Contains(t, got.series, "FROM tempo_traces_attrs_gin FINAL")
	require.NotContains(t, got.series, "_dist")
	require.Contains(t, got.series, "GROUP BY GROUPING SETS ((ts, by_custom_0), ())")
	require.NotRegexp(t, groupedByPair, got.series)

	requireReadsTheRows(t, got.instant, "tempo_traces_dist", query)
	require.Contains(t, got.instant, "GLOBAL LEFT JOIN by_attrs as by_attrs ON")

	single := renderEverySpan(t, query, false, false).series
	require.Contains(t, single, "quantilesMerge(0.5, 0.9)(q_state)")
	require.NotRegexp(t, groupedByPair, single)
}
