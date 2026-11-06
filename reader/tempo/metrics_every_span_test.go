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
	spanCount     = regexp.MustCompile(`uniqExact\(cityHash64\(main\.trace_id, main\.span_id\)\) as n`)
	countOnShards = regexp.MustCompile(`shard_rows as \( SELECT \* FROM cluster\('c', view\(.*FROM tempo_traces as main.*\)\)`)
	groupedByPair = regexp.MustCompile(`GROUP BY traces\.trace_id, traces\.span_id`)
)

// A lone `{}` count counts span hashes per bucket on each shard and sums them.
func TestEverySpanCountsOnEachShardThenSums(t *testing.T) {
	for _, tc := range []struct{ query, value, group string }{
		{`{} | rate()`, `toFloat64(sum(n)) / 30.000000 as value`, `GROUP BY ts ORDER BY ts`},
		{`({}) | rate()`, `toFloat64(sum(n)) / 30.000000 as value`, `GROUP BY ts ORDER BY ts`},
		{`{} | select(.b) | rate()`, `toFloat64(sum(n)) / 30.000000 as value`, `GROUP BY ts ORDER BY ts`},
		{`{} | count_over_time()`, `sum(n) as value`, `GROUP BY ts ORDER BY ts`},
		{`{} | count_over_time() with(sample=0.5)`, `(sum(n)) * 2 as value`, `GROUP BY ts ORDER BY ts`},
		{`{} | rate() by (name)`, `toFloat64(sum(n)) / 30.000000 as value, by_0`, `GROUP BY ts, by_0 ORDER BY ts`},
		{`{} | histogram_over_time(duration)`, `ts, bucket_ns, sum(n) as cnt`, `GROUP BY ts, bucket_ns ORDER BY ts`},
	} {
		got := renderEverySpan(t, tc.query, true, false).series
		require.Regexp(t, spanCount, got, tc.query)
		require.Regexp(t, countOnShards, got, tc.query)
		require.Contains(t, got, tc.value, tc.query)
		require.Contains(t, got, tc.group, tc.query)
		require.NotContains(t, got, "SETTINGS", tc.query)
		require.NotContains(t, got, "tempo_traces_dist", tc.query)
		require.NotRegexp(t, groupedByPair, got, tc.query)

		single := renderEverySpan(t, tc.query, false, false).series
		require.Regexp(t, spanCount, single, tc.query)
		require.NotContains(t, single, "cluster(", tc.query)
		require.Contains(t, single, tc.value, tc.query)
	}
}

// The window is an instant count's one bucket.
func TestEverySpanInstantCountHasNoBucket(t *testing.T) {
	got := renderEverySpan(t, `{} | rate() by (name)`, true, false).instant
	require.Regexp(t, spanCount, got)
	require.Contains(t, got, "toFloat64(sum(n)) / 20.000000 as value, by_0")
	require.NotContains(t, got, "intDiv")
	require.True(t, strings.HasSuffix(got, "GROUP BY by_0"), got)

	got = renderEverySpan(t, `{} | count_over_time()`, true, false).instant
	require.Contains(t, got, "SELECT sum(n) as value FROM")
	require.NotContains(t, got, "GROUP BY")
}

// Skipping unavailable shards sets the setting in the statement that reads the shards.
func TestEverySpanCountSkipsInsideTheShardRead(t *testing.T) {
	got := renderEverySpan(t, `{} | rate()`, true, true).series
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
		require.NotRegexp(t, spanCount, got, query)
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

// min and max read the rows.
func TestEverySpanMinMaxReadTheRows(t *testing.T) {
	for _, query := range []string{`{} | min_over_time(duration)`, `{} | max_over_time(duration) by (name)`} {
		got := renderEverySpan(t, query, true, true)
		for _, stmt := range []string{got.series, got.instant} {
			require.Contains(t, stmt, "FROM tempo_traces_dist as main", query)
			require.NotContains(t, stmt, "cityHash64", query)
			require.NotContains(t, stmt, "SETTINGS", query)
		}
	}
}

// avg, sum and quantiles read one row per span hash, listing only what they read.
func TestEverySpanValuesReadOneRowPerSpanHash(t *testing.T) {
	const hashGroup = "GROUP BY cityHash64(main.trace_id, main.span_id)"
	groupRe := regexp.QuoteMeta(hashGroup)
	for _, query := range []string{`{} | avg_over_time(duration)`, `{} | sum_over_time(duration)`} {
		got := renderEverySpan(t, query, true, false)
		require.Regexp(t, `FROM cluster\('c', view\( SELECT any\(main\.duration_ns\) as duration_ns, any\(main\.timestamp_ns\) as timestamp_ns FROM tempo_traces as main .*`+
			groupRe+`\)\) as main GROUP BY`, got.series, query)
		require.NotContains(t, got.series, "SETTINGS", query)
		require.NotContains(t, got.series, "metrics_spans", query)
		require.NotContains(t, got.instant, "timestamp_ns) as timestamp_ns", query)

		skip := renderEverySpan(t, query, true, true).series
		require.Regexp(t, `metrics_spans as \( SELECT \* FROM cluster\('c', view\(.*`+groupRe+`\)\)\s+SETTINGS skip_unavailable_shards\s*=\s*.?1.?\s*\) SELECT`, skip, query)

		single := renderEverySpan(t, query, false, false).series
		require.Contains(t, single, hashGroup, query)
		require.Contains(t, single, "FROM metrics_spans as main", query)
		require.NotContains(t, single, "cluster(", query)
	}
	got := renderEverySpan(t, `{} | avg_over_time(duration) by (name, resource.service.name)`, true, false).series
	require.Contains(t, got, "any(main.name) as name")
	require.Contains(t, got, "any(main.service_name) as service_name")
}

// A range quantile takes one state per bucket from each shard's span hashes and
// merges them on the coordinator, query-wide row included.
func TestEverySpanRangeQuantileMergesShardStates(t *testing.T) {
	got := renderEverySpan(t, `{} | quantile_over_time(duration, .5, .9) by (name)`, true, false)
	require.Regexp(t, `shard_rows as \( SELECT \* FROM cluster\('c', view\(.*GROUP BY cityHash64\(main\.trace_id, main\.span_id\).*quantilesState\(0\.5, 0\.9\)\(main\.duration_ns\) as q_state.*GROUP BY ts, by_0`, got.series)
	require.Contains(t, got.series, "arrayMap(x -> x / 1e9, quantilesMerge(0.5, 0.9)(q_state)) as value, grouping(ts) as query_wide, by_0 FROM shard_rows")
	require.Contains(t, got.series, "GROUP BY GROUPING SETS ((ts, by_0), ()) ORDER BY ts")
	require.NotContains(t, got.series, "SETTINGS")

	// An instant quantile has no query-wide row and reads the shards' span hashes directly.
	require.Regexp(t, `quantiles\(0\.5, 0\.9\)\(main\.duration_ns\).*FROM cluster\('c', view\(.*\)\) as main GROUP BY by_0`, got.instant)

	skip := renderEverySpan(t, `{} | quantile_over_time(duration, .5)`, true, true).series
	require.Regexp(t, `\)\)\s+SETTINGS skip_unavailable_shards\s*=\s*.?1.?\s*\) SELECT`, skip)

	single := renderEverySpan(t, `{} | quantile_over_time(duration, .5)`, false, false).series
	require.Contains(t, single, "quantilesMerge(0.5)(q_state)")
	require.NotContains(t, single, "cluster(")
}

// A custom by-label pivots on each shard over its local attributes.
func TestEverySpanCustomByStaysOnTheShard(t *testing.T) {
	got := renderEverySpan(t, `{} | rate() by (.a, name) with(sample=0.5)`, true, false)
	require.Regexp(t, spanCount, got.series)
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

// Functions that read a span's value keep the grouped read under a custom by-label.
func TestEverySpanValuesKeepTheGroupedReadUnderCustomBy(t *testing.T) {
	for _, query := range []string{`{} | avg_over_time(duration) by (.a)`, `{} | max_over_time(duration) by (.a)`,
		`{} | quantile_over_time(duration, .5) by (.a)`} {
		got := renderEverySpan(t, query, true, false).series
		require.Regexp(t, groupedByPair, got, query)
	}
}
