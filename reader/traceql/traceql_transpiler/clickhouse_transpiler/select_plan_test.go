package clickhouse_transpiler

import (
	"strings"
	"testing"

	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
)

// select() filters nothing: it plans no stage, at any position.
func TestSelectStagesPlanNoStage(t *testing.T) {
	for _, c := range []struct {
		query, search, metrics string
	}{
		{`{.a = 1} | select(.x)`, `{.a}`, `{.a}`},
		{`{.a = 1} | select(.x) | count() > 1 | select(duration)`, `{.a}|count() > 1`, `{.a}|count() > 1`},
		{`({.a = 1} | select(.x)) >> {.b = 2}`, `>>({.a},{.b})`, `>>({.a},{.b})`},
		{`select(.x) | {.a = 1}`, `{}|{.a}`, `{}|{.a}`},
		// A lone {} keeps the newest-traces pre-pick.
		{`select(.x)`, `attrless`, `{}`},
		{`{} | select(.x)`, `attrless`, `{}`},
	} {
		t.Run(c.query, func(t *testing.T) {
			_, expr := searchStages(t, planSearch(t, c.query))
			if got := describePlan(t, expr); got != c.search {
				t.Errorf("search plan = %s, want %s", got, c.search)
			}
			_, expr = metricsResult(t, planMetrics(t, c.query))
			if got := describePlan(t, expr); got != c.metrics {
				t.Errorf("metrics plan = %s, want %s", got, c.metrics)
			}
		})
	}
}

func TestAllSpansIgnoresSelect(t *testing.T) {
	for query, want := range map[string]bool{
		`select(.x)`:               true,
		`{} | select(.x)`:          true,
		`({}) | select(.x, .y)`:    true,
		`{.a = 1} | select(.x)`:    false,
		`select(.x) | count() > 1`: false,
	} {
		if got := AllSpans(mustParseQuery(t, query)); got != want {
			t.Errorf("AllSpans(%s) = %v, want %v", query, got, want)
		}
	}
}

// Search reads the selected attributes of the listed spans from the attribute
// index, joined on (trace_id, span_id); intrinsics read from the span row,
// duration and the trace-level root intrinsics, read nothing.
func TestSearchReadsTheSelectedValuesOfTheListedSpans(t *testing.T) {
	res := renderPlan(t, planSearch(t,
		`{.a = 1} | select(.x, span.y, resource.y, name, span:name, status, kind, statusMessage, duration, rootName, rootServiceName) | select(.z)`),
		testPlannerContext(3))
	for _, want := range []string{
		`key IN ('x','y','name','status','kind','statusMessage','z','a')`,
		`((traces_idx.trace_id, traces_idx.span_id) IN (trace_span_ids))`,
		`((traces_idx.timestamp_ns) > (86390000000000)) and ((traces_idx.timestamp_ns) <= (86410000000000)) and (traces_idx.trace_id IN (trace_ids))`,
		`any left JOIN selected_attrs ON ((traces.trace_id) == (selected_attrs.trace_id)) and ((traces.span_id) == (selected_attrs.span_id))`,
		`groupArray(selected_attrs.keys) as span_attr_keys`,
		`groupArray(selected_attrs.vals) as span_attr_vals`,
	} {
		if !strings.Contains(res, want) {
			t.Errorf("%q missing:\n%s", want, res)
		}
	}
}

// Search without select() or filtered fields reads no values.
func TestSearchWithoutKeysReadsNoValues(t *testing.T) {
	for _, query := range []string{`{}`, `count() > 1`, `by(.b * 2) | by(duration)`, `{duration > 1s} | avg(.b) > 1`, `{rootName = "r"} !~ {}`} {
		res := renderPlan(t, planSearch(t, query), testPlannerContext(3))
		if strings.Contains(res, "selected_attrs") || strings.Contains(res, "selected_nested_set") {
			t.Errorf("%s reads selected values:\n%s", query, res)
		}
		if !strings.Contains(res, `CAST([], 'Array(Array(String))') as span_attr_keys, CAST([], 'Array(Array(String))') as span_attr_vals`) {
			t.Errorf("%s lacks the empty selected-value columns:\n%s", query, res)
		}
	}
}

// Search echoes the fields every spanset filter reads as if selected: the
// union across the query, next to the select() fields.
func TestSearchEchoesFilteredFields(t *testing.T) {
	for query, want := range map[string]string{
		`{.a = 1}`: `a`,
		`{.a = 1 && name = "n"} >> {span.b = 2 || status = error} | select(.x)`: `x,a,name,b,status`,
		`{.a = 1} | count() > 1 | {kind = server && .a != nil}`:                 `a,kind`,
		`{nestedSetParent < 0} &>> {nestedSetRight - nestedSetLeft = 1}`:        `nestedSetParent,nestedSetRight,nestedSetLeft`,
		`{duration > 1s && rootServiceName = "s" && traceDuration > 1s}`:        ``,
		`{.nestedSetLeft = "x"}`: `nestedSetLeft`,
	} {
		keys, err := spanValueKeys(mustParseQuery(t, query))
		if err != nil || strings.Join(keys, ",") != want {
			t.Errorf("spanValueKeys(%s) = %v, %v; want %s", query, keys, err, want)
		}
	}
}

// A root filter echoes nestedSetParent from the side table.
func TestSearchEchoesRootFilter(t *testing.T) {
	res := renderPlan(t, planSearch(t, `{nestedSetParent < 0 && .a = 1}`), testPlannerContext(3))
	for _, want := range []string{
		`(key IN ('a'))`,
		`arraySort(arrayConcat(selected_attrs.kv, [('nestedSetParent', toString(ifNull(selected_nested_set.parent_, 0)))]))`,
	} {
		if !strings.Contains(res, want) {
			t.Errorf("%q missing:\n%s", want, res)
		}
	}
}

func TestSelectedValuesReadDistributedTablesOnClusters(t *testing.T) {
	ctx := testPlannerContext(3)
	ctx.IsCluster = true
	res := renderPlan(t, planSearch(t, `{.a = 1} | select(.x)`), ctx)
	for _, want := range []string{
		`FROM tempo_traces_attrs_gin_dist as traces_idx`,
		`(traces_idx.trace_id, traces_idx.span_id) GLOBAL IN (trace_span_ids)`,
		`global any left JOIN selected_attrs`,
	} {
		if !strings.Contains(res, want) {
			t.Errorf("%q missing:\n%s", want, res)
		}
	}
}

// Search rejects the intrinsics gigapipe cannot read; metrics ignore select().
func TestSelectUnsupportedIntrinsics(t *testing.T) {
	q := mustParseQuery(t, `{} | select(.x, span:parentID)`)
	if _, err := Plan(q); err == nil || err.Error() != "unsupported attribute span:parentID" {
		t.Errorf("search error = %v", err)
	}
	q = mustParseQuery(t, `{} | select(.x, span:parentID) | rate()`)
	if _, err := PlanMetrics(q, countSpans); err != nil {
		t.Errorf("metrics error = %v", err)
	}
}

func TestSpanValueKeysSkipListedIntrinsics(t *testing.T) {
	q, err := traceql_ir.Parse(`{} | select(rootName, duration, trace:rootService, span:duration, traceDuration, trace:id, span:id)`)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := spanValueKeys(q)
	if err != nil || len(keys) != 0 {
		t.Errorf("spanValueKeys = %v, %v; want none", keys, err)
	}
}

// Search reads the selected nested-set values of every listed span from the
// side table, 0 for a span without a row, next to the selected attributes.
func TestSearchReadsSelectedNestedSetValues(t *testing.T) {
	res := renderPlan(t, planSearch(t, `{.a = 1} | select(nestedSetRight, .x, nestedSetLeft)`), testPlannerContext(3))
	for _, want := range []string{
		`key IN ('x','a')`,
		`argMax(ns.left, ns.version) as left_`,
		`FROM tempo_traces_nested_set as ns WHERE (ns.trace_id IN (trace_ids)) and ((ns.trace_id, ns.span_id) IN (trace_span_ids))`,
		`any left JOIN selected_nested_set ON ((traces.trace_id) == (selected_nested_set.trace_id)) and ((traces.span_id) == (selected_nested_set.span_id))`,
		`arraySort(arrayConcat(selected_attrs.kv, [('nestedSetRight', toString(ifNull(selected_nested_set.right_, 0))), ('nestedSetLeft', toString(ifNull(selected_nested_set.left_, 0)))]))`,
	} {
		if !strings.Contains(res, want) {
			t.Errorf("%q missing:\n%s", want, res)
		}
	}
	if strings.Contains(res, "parent_") {
		t.Errorf("reads an unselected field:\n%s", res)
	}
}

// Without selected attributes, only the side table is read.
func TestSearchReadsOnlySelectedNestedSetValues(t *testing.T) {
	res := renderPlan(t, planSearch(t, `{} | select(nestedSetParent) | count() > 0`), testPlannerContext(3))
	if strings.Contains(res, "selected_attrs") {
		t.Errorf("reads attributes:\n%s", res)
	}
	want := `groupArray(arrayMap(x -> x.1, arraySort([('nestedSetParent', toString(ifNull(selected_nested_set.parent_, 0)))]))) as span_attr_keys`
	if !strings.Contains(res, want) {
		t.Errorf("%q missing:\n%s", want, res)
	}
}

// On a cluster the side table is read through its Distributed table.
func TestSelectedNestedSetValuesReadDistributedTablesOnClusters(t *testing.T) {
	ctx := testPlannerContext(3)
	ctx.IsCluster = true
	res := renderPlan(t, planSearch(t, `{} | select(nestedSetLeft)`), ctx)
	for _, want := range []string{
		`FROM tempo_traces_nested_set_dist as ns WHERE (ns.trace_id GLOBAL IN (trace_ids))`,
		`global any left JOIN selected_nested_set`,
	} {
		if !strings.Contains(res, want) {
			t.Errorf("%q missing:\n%s", want, res)
		}
	}
}

// An attribute named like a nested-set intrinsic selects the intrinsic.
func TestSelectedAttributeSharesNestedSetKey(t *testing.T) {
	q, err := traceql_ir.Parse(`{} | select(.nestedSetLeft, span.nestedSetRight, nestedSetLeft)`)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := spanValueKeys(q)
	if err != nil || strings.Join(keys, ",") != "nestedSetLeft,nestedSetRight" {
		t.Errorf("spanValueKeys = %v, %v", keys, err)
	}
}
