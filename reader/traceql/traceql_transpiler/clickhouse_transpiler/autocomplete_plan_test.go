package clickhouse_transpiler

import (
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
)

// autocompletePlans plans tags v2 and values v2 of key x for query.
func autocompletePlans(t *testing.T, query string) map[string]shared.SQLRequestPlanner {
	t.Helper()
	q, err := traceql_ir.Parse(query)
	if err != nil {
		t.Fatal(err)
	}
	tags, err := PlanTagsV2(q)
	if err != nil {
		t.Fatal(err)
	}
	values, err := PlanValuesV2(q, "x")
	if err != nil {
		t.Fatal(err)
	}
	return map[string]shared.SQLRequestPlanner{"tags": tags, "values": values}
}

// unwrapAutocompletePlan unwraps an autocomplete plan down to its evaluated expression.
func unwrapAutocompletePlan(t *testing.T, plan shared.SQLRequestPlanner) (structural bool, expr shared.SQLRequestPlanner) {
	t.Helper()
	if s, ok := plan.(*StructuralQueryPlanner); ok {
		structural = true
		plan = s.Main
	}
	switch p := plan.(type) {
	case *SelectTagsPlanner:
		return structural, p.Result
	case *SelectValuesPlanner:
		return structural, p.Result
	}
	t.Fatalf("autocomplete plan is %T", plan)
	return false, nil
}

// filteredQueries are the search queries that select some spans.
func filteredQueries() []string {
	return slices.DeleteFunc(slices.Clone(searchQueries), func(q string) bool { return q == `{}` })
}

// Autocomplete suggests from the spans search returns: the same evaluated expression.
func TestAutocompleteReadsTheSearchResultSpans(t *testing.T) {
	for _, query := range filteredQueries() {
		_, searchExpr := searchStages(t, planSearch(t, query))
		want := describePlan(t, searchExpr)
		for kind, plan := range autocompletePlans(t, query) {
			t.Run(kind+" "+query, func(t *testing.T) {
				structural, expr := unwrapAutocompletePlan(t, plan)
				if got := describePlan(t, expr); got != want {
					t.Errorf("autocomplete plan %s, search plan %s", got, want)
				}
				if structural != slices.Contains(structuralQueries, query) {
					t.Errorf("structural gate = %v", structural)
				}
			})
		}
	}
}

func TestAutocompleteWithoutSelectorReadsEverySpan(t *testing.T) {
	for _, query := range []string{``, `{}`, `({})`, `{} | rate()`} {
		var q *traceql_ir.Query
		if query != "" {
			var err error
			if q, err = traceql_ir.Parse(query); err != nil {
				t.Fatal(err)
			}
		}
		if !AllSpans(q) {
			t.Errorf("AllSpans(%q) = false", query)
		}
		tags, err := PlanTagsV2(q)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := tags.(*AllTagsRequestPlanner); !ok {
			t.Errorf("tags v2 of %q is %T, want every key", query, tags)
		}
		values, err := PlanValuesV2(q, "x")
		if err != nil {
			t.Fatal(err)
		}
		if v, ok := values.(*AllValuesRequestPlanner); !ok || v.Key != "x" {
			t.Errorf("values v2 of %q is %#v, want every value of x", query, values)
		}
	}
	for _, query := range filteredQueries() {
		q, err := traceql_ir.Parse(query)
		if err != nil {
			t.Fatal(err)
		}
		if AllSpans(q) {
			t.Errorf("AllSpans(%q) = true", query)
		}
	}
}

func TestAutocompleteJoinsResultSpansOnTraceAndSpan(t *testing.T) {
	join := regexp.MustCompile(`\(traces_idx\.trace_id, traces_idx\.span_id\) IN \(_\d+result_span_ids\)`)
	for _, query := range filteredQueries() {
		for kind, plan := range autocompletePlans(t, query) {
			t.Run(kind+" "+query, func(t *testing.T) {
				res := renderPlan(t, plan, testPlannerContext(3))
				if !join.MatchString(res) {
					t.Errorf("attributes are not read by (trace_id, span_id):\n%s", res)
				}
			})
		}
	}
}

func TestAutocompleteSpansAreUncapped(t *testing.T) {
	capped := regexp.MustCompile(`group(Uniq)?Array\(\d+\)|arraySlice\(`)
	for _, query := range filteredQueries() {
		for kind, plan := range autocompletePlans(t, query) {
			t.Run(kind+" "+query, func(t *testing.T) {
				res := renderPlan(t, plan, testPlannerContext(3))
				if m := capped.FindString(res); m != "" {
					t.Errorf("result spans capped by %s:\n%s", m, res)
				}
				if n := strings.Count(res, "LIMIT"); n != 1 || !strings.HasSuffix(strings.TrimSuffix(res, " SETTINGS enable_analyzer=1"), " LIMIT 5") {
					t.Errorf("want only the suggestion limit, got %d limits:\n%s", n, res)
				}
			})
		}
	}
}

func TestAutocompleteSuggestsDistinctSortedKeysAndValues(t *testing.T) {
	plans := autocompletePlans(t, `{.a = 1} >> {.b = 2}`)
	tags := renderPlan(t, plans["tags"], testPlannerContext(3))
	if !regexp.MustCompile(`GROUP BY key ORDER BY key asc +LIMIT 5`).MatchString(tags) {
		t.Errorf("tags are not distinct keys in order:\n%s", tags)
	}
	values := renderPlan(t, plans["values"], testPlannerContext(3))
	if !regexp.MustCompile(`GROUP BY val ORDER BY val asc +LIMIT 5`).MatchString(values) || !strings.Contains(values, "(key) == ('x')") {
		t.Errorf("values are not distinct values of x in order:\n%s", values)
	}
}

func TestAutocompleteSpanStartIsLeftOpenRightClosed(t *testing.T) {
	for _, query := range filteredQueries() {
		for kind, plan := range autocompletePlans(t, query) {
			t.Run(kind+" "+query, func(t *testing.T) {
				res := renderPlan(t, plan, testPlannerContext(3))
				bounds := spanStartBounds(res)
				if len(bounds) == 0 {
					t.Fatalf("no span start bounds:\n%s", res)
				}
				for _, b := range bounds {
					if b != ">86390" && b != "<=86410" {
						t.Errorf("span start bound %s, want >86390 or <=86410:\n%s", b, res)
					}
				}
			})
		}
	}
}

// The root filter applies on every evaluation of a plan.
func TestAutocompleteRootFilterAppliesToTheResultSpans(t *testing.T) {
	for kind, plan := range autocompletePlans(t, `{nestedSetParent < 0 && .a = 1}`) {
		for i := 0; i < 2; i++ {
			res := renderPlan(t, plan, testPlannerContext(3))
			if !strings.Contains(res, rootColumn) {
				t.Errorf("%s, evaluation %d: root filter missing from the result spans:\n%s", kind, i+1, res)
			}
		}
	}
}

func TestAutocompleteStructuralQueriesNeedClickHouse249AndTheAnalyzer(t *testing.T) {
	old := testPlannerContext(3)
	old.VersionInfo = map[string]int64{}
	for _, query := range filteredQueries() {
		structural := slices.Contains(structuralQueries, query)
		for kind, plan := range autocompletePlans(t, query) {
			_, err := plan.Process(old)
			if structural && err != errStructuralVersion {
				t.Errorf("%s %s: error = %v, want %v", kind, query, err, errStructuralVersion)
			}
			if !structural && err != nil {
				t.Errorf("%s %s: %v", kind, query, err)
			}
			res := renderPlan(t, plan, testPlannerContext(3))
			if strings.HasSuffix(res, " SETTINGS enable_analyzer=1") != structural ||
				strings.Count(res, "enable_analyzer") != map[bool]int{true: 1}[structural] {
				t.Errorf("%s %s: enable_analyzer pinned wrongly:\n%s", kind, query, res)
			}
		}
	}
}

func TestAutocompleteReadsDistributedTablesOnClusters(t *testing.T) {
	local := regexp.MustCompile(`\btempo_traces(_attrs_gin)?\b[^_]`)
	plainIn := regexp.MustCompile(`[^L] IN \((trace_ids|\w*result_span_ids|attrless_trace_ids| SELECT trace_id FROM \w+)\)`)
	for _, query := range filteredQueries() {
		for kind, plan := range autocompletePlans(t, query) {
			t.Run(kind+" "+query, func(t *testing.T) {
				ctx := testPlannerContext(3)
				ctx.IsCluster = true
				res := renderPlan(t, plan, ctx)
				if m := local.FindString(res); m != "" {
					t.Errorf("reads a local table (%q):\n%s", m, res)
				}
				if m := plainIn.FindString(res); m != "" {
					t.Errorf("distributed subquery under a plain IN (%q):\n%s", m, res)
				}
			})
		}
	}
}
