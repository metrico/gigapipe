package service

import (
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/tempo"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/stretchr/testify/require"
)

func TestMetricsCompareRequest(t *testing.T) {
	ctx := &shared.PlannerContext{
		From: time.Unix(100, 0),
		To:   time.Unix(200, 0),
	}
	q, err := traceql_ir.Parse(`{.a = "x"} | compare({.b = "y" || .c = 3}, 5)`)
	require.NoError(t, err)
	got := compareRequest(q, ctx)
	require.Equal(t, 5, got.TopN)
	require.Equal(t, int64(100e9), got.BaselineFromNS)
	require.Equal(t, int64(200e9), got.BaselineToNS)

	q, err = traceql_ir.Parse(`{} | compare({.b = "y"}, 2, 50000000000, 150000000000)`)
	require.NoError(t, err)
	ctx.IsCluster = true
	got = compareRequest(q, ctx)
	require.Equal(t, int64(50e9), got.BaselineFromNS)
	require.Equal(t, int64(150e9), got.BaselineToNS)
	require.Equal(t, int64(100e9), got.FromNS)
}

// compareView splits a cluster's compare statement into the coordinator's part
// before the shard counts, the counts, and the part after them.
var compareView = regexp.MustCompile(`(?s)^(.*)FROM cluster\('c', view\((.*)\)\) SETTINGS prefer_localhost_replica=0\)(.*)$`)

// On a cluster, the tempo compare builders count attribute values of the
// baseline traces on each shard; the coordinator sums them and keeps the topN.
func TestCompareCountsOnEachShardThenRanks(t *testing.T) {
	ctx := &shared.PlannerContext{
		IsCluster:            true,
		ClusterName:          "c",
		DistSuffix:           "_dist",
		From:                 time.Unix(86390, 0).UTC(),
		To:                   time.Unix(86410, 0).UTC(),
		TracesAttrsTable:     "tempo_traces_attrs_gin",
		TracesAttrsDistTable: "tempo_traces_attrs_gin_dist",
		TracesTable:          "tempo_traces",
		TracesDistTable:      "tempo_traces_dist",
		VersionInfo:          map[string]int64{dbversion.CapStructuralQueries: 0},
	}
	attrs := regexp.MustCompile(`FROM tempo_traces_attrs_gin FINAL WHERE .*\(trace_id IN \(_\d+trace_ids\)\)`)
	for _, query := range []string{`{.a = 1} | compare({.c = 3})`, `({.a = 1} >> {.b = 2}) | compare({})`} {
		q, err := traceql_ir.Parse(query)
		require.NoError(t, err)
		creq := compareRequest(q, ctx)
		plan, err := clickhouse_transpiler.PlanCompare(q, q.Metrics.Compare.Filter,
			time.Unix(0, creq.BaselineFromNS), time.Unix(0, creq.BaselineToNS), compareBuild(creq))
		require.NoError(t, err)
		stmt, err := plan.Metrics.Process(ctx)
		require.NoError(t, err)
		res, err := stmt.String(sql.DefaultCtx())
		require.NoError(t, err)
		m := compareView.FindStringSubmatch(res)
		require.NotNil(t, m, "%s: counts are not run on each shard:\n%s", query, res)
		before, shard, after := m[1], m[2], m[3]
		require.Regexp(t, attrs, shard, query)
		require.Regexp(t, `sel_n.*base_n.* GROUP BY attr_key, attr_val$`, shard, query)
		require.True(t, strings.HasSuffix(before, "compare_counts as ( SELECT attr_key, attr_val, sel_n, base_n "), "%s:\n%s", query, res)
		for _, coord := range []string{"sum(sel_n) as selection_count", "sum(base_n) as baseline_count", " HAVING (((selection_count)", "ranked as (", "(rn) <= (10)"} {
			require.Contains(t, after, coord, query)
			require.NotContains(t, shard, coord, query)
		}
		require.NotContains(t, before+after, "tempo_traces", query)
	}
}

// compare() probes its baseline window; every other query its own range.
func TestUnresolvedProbeWindow(t *testing.T) {
	ctx := &shared.PlannerContext{From: time.Unix(100, 0), To: time.Unix(200, 0)}

	for _, query := range []string{
		`{.a = "1"} >> {.b = "1"} | rate()`,
		`{.a = "1"} >> {.b = "1"} | compare({.c = "1"})`,
	} {
		q, err := traceql_ir.Parse(query)
		require.NoError(t, err)
		got := unresolvedProbeContext(q, ctx)
		require.True(t, got.From.Equal(ctx.From) && got.To.Equal(ctx.To), query)
	}

	q, err := traceql_ir.Parse(`{.a = "1"} >> {.b = "1"} | compare({.c = "1"}, 2, 50000000000, 150000000000)`)
	require.NoError(t, err)
	got := unresolvedProbeContext(q, ctx)
	require.True(t, got.From.Equal(time.Unix(50, 0)), got.From)
	require.True(t, got.To.Equal(time.Unix(150, 0)), got.To)
	require.True(t, ctx.From.Equal(time.Unix(100, 0)), "planner context changed")
}

// exemplars=N keeps N exemplars per series, evenly spread over those in
// time order; false and N < 1 keep none; no hint keeps all.
func TestCapExemplars(t *testing.T) {
	ten := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	for _, c := range []struct {
		hints string
		in    []int
		want  []int
	}{
		{``, ten, ten},
		{` with(exemplars=true)`, ten, ten},
		{` with(exemplars=false)`, ten, nil},
		{` with(exemplars=0)`, ten, nil},
		{` with(exemplars=10)`, ten, ten},
		{` with(exemplars=20)`, ten, ten},
		{` with(exemplars=1)`, ten, []int{5}},
		{` with(exemplars=2)`, ten, []int{2, 7}},
		{` with(exemplars=3)`, ten, []int{1, 5, 8}},
		{` with(exemplars=4)`, []int{0, 1, 2, 3, 4, 5}, []int{0, 2, 3, 5}},
		{` with(exemplars=2)`, nil, nil},
	} {
		q, err := traceql_ir.Parse(`{} | rate()` + c.hints)
		if err != nil {
			t.Fatal(err)
		}
		if got := capExemplars(q, c.in); !slices.Equal(got, c.want) {
			t.Errorf("%s of %v = %v, want %v", c.hints, c.in, got, c.want)
		}
	}
}

// with(sample=f) reaches metrics and compare() requests as 1/f, under span and
// trace hashing alike; any other hint, or one that keeps no spans, leaves them
// at 1.
func TestSampleHintSetsRequestScale(t *testing.T) {
	ctx := &shared.PlannerContext{From: time.Unix(100, 0), To: time.Unix(200, 0)}
	req := &model.MetricsQueryRequest{Step: time.Second}
	for _, tc := range []struct {
		selector string
		hint     string
		want     float64
	}{
		{`{.a = 1}`, ` with(sample=0.25)`, 4},
		{`{.a = 1} >> {.b = 2}`, ` with(sample=0.25)`, 4},
		{`{.a = 1}`, ``, 1},
		{`{.a = 1}`, ` with(sample=1)`, 1},
		{`{.a = 1}`, ` with(sample=true)`, 1},
		{`{.a = 1}`, ` with(sample=0.` + strings.Repeat("0", 319) + `1)`, 1},
	} {
		q, err := traceql_ir.Parse(tc.selector + ` | rate()` + tc.hint)
		require.NoError(t, err)
		require.Equal(t, tc.want, metricsRequest(q, req, ctx).Scale, q)

		q, err = traceql_ir.Parse(tc.selector + ` | compare({.c = 3})` + tc.hint)
		require.NoError(t, err)
		require.Equal(t, tc.want, compareRequest(q, ctx).Scale, q)
	}
}

// A hint that keeps no spans renders the aggregate unscaled.
func TestHintKeepingNoSpansRendersUnscaled(t *testing.T) {
	ctx := &shared.PlannerContext{From: time.Unix(100, 0), To: time.Unix(200, 0)}
	spans := sql.NewWith(sql.NewSelect().Select(sql.NewRawObject("trace_id")).From(sql.NewRawObject("result")), "spans")
	for name, build := range map[string]func(*tempo.MetricsRequest) (sql.ISelect, []string, error){
		"range":   tempo.BuildMetricsRangeQuery,
		"instant": tempo.BuildMetricsInstantQuery,
	} {
		for _, step := range []time.Duration{time.Millisecond, time.Second, 15 * time.Second} {
			req := &model.MetricsQueryRequest{Step: step}
			render := func(query string) string {
				q, err := traceql_ir.Parse(query)
				require.NoError(t, err)
				sel, _, err := build(over(metricsRequest(q, req, ctx), spans))
				require.NoError(t, err)
				str, err := sel.String(sql.DefaultCtx())
				require.NoError(t, err)
				return str
			}
			for _, fn := range []string{`rate()`, `count_over_time()`, `sum_over_time(duration)`} {
				query := `{.a = 1} | ` + fn
				for _, zeros := range []int{20, 305, 307, 319} {
					got := render(query + ` with(sample=0.` + strings.Repeat("0", zeros) + `1)`)
					require.Equal(t, render(query), got, "%s %s %d zeros at %s", name, fn, zeros, step)
				}
			}
		}
	}
}

// The smallest hint that keeps spans folds rate's divisor to a normal double
// at a small step.
func TestSmallestKeptSampleHintRendersNormalDivisor(t *testing.T) {
	ctx := &shared.PlannerContext{From: time.Unix(100, 0), To: time.Unix(200, 0)}
	req := &model.MetricsQueryRequest{Step: time.Millisecond}
	spans := sql.NewWith(sql.NewSelect().Select(sql.NewRawObject("trace_id")).From(sql.NewRawObject("result")), "spans")
	q, err := traceql_ir.Parse(`{.a = 1} | rate() with(sample=0.0000000000000000000542101086242752217003726400434970855712890625)`)
	require.NoError(t, err)
	sel, _, err := tempo.BuildMetricsRangeQuery(over(metricsRequest(q, req, ctx), spans))
	require.NoError(t, err)
	str, err := sel.String(sql.DefaultCtx())
	require.NoError(t, err)

	m := regexp.MustCompile(`count\(\*\)\) / ([0-9.e+-]+)`).FindStringSubmatch(str)
	require.NotNil(t, m, str)
	divisor, err := strconv.ParseFloat(m[1], 64)
	require.NoError(t, err)
	require.GreaterOrEqual(t, divisor, 0x1p-1022, str)
}

// Exemplars of rate and count_over_time sit on the scaled series point of their
// step; those of other functions keep their span's duration, unscaled.
func TestExemplarValueUnderScaling(t *testing.T) {
	const (
		stepMs = int64(1000)
		tsMs   = int64(5500)
		durNs  = int64(1500e6)
	)
	scaledSeries := map[int64]float64{5000: 8, 6000: 12}
	for fn, want := range map[string]float64{
		"rate":               8,
		"count_over_time":    8,
		"sum_over_time":      1.5,
		"min_over_time":      1.5,
		"max_over_time":      1.5,
		"avg_over_time":      1.5,
		"quantile_over_time": 1.5,
	} {
		require.Equal(t, want, exemplarValue(exemplarHasDuration(fn), durNs, tsMs, stepMs, scaledSeries), fn)
	}
	require.Zero(t, exemplarValue(false, durNs, tsMs, 0, scaledSeries))
}
