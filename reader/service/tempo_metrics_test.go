package service

import (
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
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
	got := compareRequest(q.Metrics, ctx)
	require.Equal(t, 5, got.TopN)
	require.Equal(t, int64(100e9), got.BaselineFromNS)
	require.Equal(t, int64(200e9), got.BaselineToNS)

	q, err = traceql_ir.Parse(`{} | compare({.b = "y"}, 2, 50000000000, 150000000000)`)
	require.NoError(t, err)
	got = compareRequest(q.Metrics, ctx)
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
		creq := compareRequest(q.Metrics, ctx)
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
		for _, coord := range []string{"sum(sel_n) as selection_count", "sum(base_n) as baseline_count", " HAVING ", "ranked as (", "(rn) <= (10)"} {
			require.Contains(t, after, coord, query)
			require.NotContains(t, shard, coord, query)
		}
		require.NotContains(t, before+after, "tempo_traces", query)
	}
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
