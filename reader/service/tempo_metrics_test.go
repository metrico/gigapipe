package service

import (
	"slices"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	"github.com/stretchr/testify/require"
)

func TestMetricsCompareRequest(t *testing.T) {
	ctx := &shared.PlannerContext{
		From:                 time.Unix(100, 0),
		To:                   time.Unix(200, 0),
		TracesAttrsTable:     "attrs",
		TracesAttrsDistTable: "attrs_dist",
	}
	q, err := traceql_ir.Parse(`{.a = "x"} | compare({.b = "y" || .c = 3}, 5)`)
	require.NoError(t, err)
	got := compareRequest(q.Metrics, ctx)
	require.Equal(t, 5, got.TopN)
	require.Equal(t, int64(100e9), got.BaselineFromNS)
	require.Equal(t, int64(200e9), got.BaselineToNS)
	require.Equal(t, "attrs", got.AttrsTable)

	q, err = traceql_ir.Parse(`{} | compare({.b = "y"}, 2, 50000000000, 150000000000)`)
	require.NoError(t, err)
	ctx.IsCluster = true
	got = compareRequest(q.Metrics, ctx)
	require.Equal(t, int64(50e9), got.BaselineFromNS)
	require.Equal(t, int64(150e9), got.BaselineToNS)
	require.Equal(t, int64(100e9), got.FromNS)
	require.Equal(t, "attrs_dist", got.AttrsTable)
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
