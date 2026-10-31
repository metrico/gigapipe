package clickhouse_transpiler

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
)

// autocompletePlans plans tags v2 and values v2 of key x for query.
func autocompletePlans(t *testing.T, query string) map[string]*SearchPlan {
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
	return map[string]*SearchPlan{"tags": tags, "values": values}
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
				structural, expr := idsResult(t, plan.Ids[0].Statement)
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
		if _, ok := tags.Data.(*AllTagsRequestPlanner); !ok || len(tags.Ids) != 0 {
			t.Errorf("tags v2 of %q is %#v, want every key in one statement", query, tags)
		}
		values, err := PlanValuesV2(q, "x")
		if err != nil {
			t.Fatal(err)
		}
		if v, ok := values.Data.(*AllValuesRequestPlanner); !ok || v.Key != "x" || len(values.Ids) != 0 {
			t.Errorf("values v2 of %q is %#v, want every value of x in one statement", query, values)
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

// The search statement lists the result span ids by trace and never reads SearchResultTable.
func TestAutocompleteSearchListsSpanIdsByTrace(t *testing.T) {
	ids := regexp.MustCompile(`SELECT trace_id, groupArray\(span_id\) as span_ids FROM \w+ GROUP BY trace_id( SETTINGS enable_analyzer=1)?$`)
	for _, query := range filteredQueries() {
		for kind, plan := range autocompletePlans(t, query) {
			for _, cluster := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s %s/cluster=%t", kind, query, cluster), func(t *testing.T) {
					if cluster && slices.Contains(structuralQueries, query) {
						t.Skip("on a cluster TestStructuralIdsStatementsRunWholeOnEachShard covers it")
					}
					ctx := testPlannerContext(3)
					ctx.IsCluster = cluster
					res := renderPlan(t, plan.Ids[0].Statement, ctx)
					if !strings.HasPrefix(res, "WITH") || !ids.MatchString(res) {
						t.Errorf("search is not (trace_id, groupArray(span_id)) by trace:\n%s", res)
					}
					if strings.Contains(res, SearchResultTable) {
						t.Errorf("search reads %s:\n%s", SearchResultTable, res)
					}
				})
			}
		}
	}
}

// The data statement reads the attributes of SearchResultTable's spans alone,
// with plain IN on a cluster.
func TestAutocompleteDataReadsOnlySearchResult(t *testing.T) {
	plainInRe := regexp.MustCompile(`[^L] IN \(_\d+(trace_ids|trace_span_ids)\)`)
	spansTable := regexp.MustCompile(`\btempo_traces(_dist)?\b`)
	for _, query := range filteredQueries() {
		for kind, plan := range autocompletePlans(t, query) {
			for _, cluster := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s %s/cluster=%t", kind, query, cluster), func(t *testing.T) {
					ctx := testPlannerContext(3)
					ctx.IsCluster = cluster
					res := renderPlan(t, plan.Data, ctx)
					if !strings.Contains(res, "FROM "+SearchResultTable) {
						t.Errorf("does not read %s:\n%s", SearchResultTable, res)
					}
					for _, banned := range []string{"result_spans", "spansets", "index_grouped", "GLOBAL", "enable_analyzer"} {
						if strings.Contains(res, banned) {
							t.Errorf("reads %s:\n%s", banned, res)
						}
					}
					if m := spansTable.FindString(res); m != "" {
						t.Errorf("reads %s:\n%s", m, res)
					}
					if n := len(plainInRe.FindAllString(res, -1)); n != 2 {
						t.Errorf("%d plain IN over the search result, want trace and span:\n%s", n, res)
					}
					attrs := map[bool]string{false: "tempo_traces_attrs_gin as", true: "tempo_traces_attrs_gin_dist as"}[cluster]
					if !strings.Contains(res, attrs) {
						t.Errorf("does not read %s:\n%s", attrs, res)
					}
				})
			}
		}
	}
}

func TestAutocompleteJoinsResultSpansOnTraceAndSpan(t *testing.T) {
	join := regexp.MustCompile(`\(traces_idx\.trace_id, traces_idx\.span_id\) IN \(_\d+trace_span_ids\)`)
	for _, query := range filteredQueries() {
		for kind, plan := range autocompletePlans(t, query) {
			t.Run(kind+" "+query, func(t *testing.T) {
				res := renderPlan(t, plan.Data, testPlannerContext(3))
				if !join.MatchString(res) {
					t.Errorf("attributes are not read by (trace_id, span_id):\n%s", res)
				}
			})
		}
	}
}

func TestAutocompleteSpansAreUncapped(t *testing.T) {
	capped := regexp.MustCompile(`group(Uniq)?Array\(\d+\)|arraySlice\(|LIMIT`)
	for _, query := range filteredQueries() {
		for kind, plan := range autocompletePlans(t, query) {
			t.Run(kind+" "+query, func(t *testing.T) {
				res := renderPlan(t, plan.Ids[0].Statement, testPlannerContext(3))
				if m := capped.FindString(res); m != "" {
					t.Errorf("result spans capped by %s:\n%s", m, res)
				}
				data := renderPlan(t, plan.Data, testPlannerContext(3))
				if n := strings.Count(data, "LIMIT"); n != 1 || !strings.HasSuffix(data, " LIMIT 5") {
					t.Errorf("want only the suggestion limit, got %d limits:\n%s", n, data)
				}
			})
		}
	}
}

func TestAutocompleteSuggestsDistinctSortedKeysAndValues(t *testing.T) {
	plans := autocompletePlans(t, `{.a = 1} >> {.b = 2}`)
	tags := renderPlan(t, plans["tags"].Data, testPlannerContext(3))
	if !regexp.MustCompile(`GROUP BY key ORDER BY key asc +LIMIT 5`).MatchString(tags) {
		t.Errorf("tags are not distinct keys in order:\n%s", tags)
	}
	values := renderPlan(t, plans["values"].Data, testPlannerContext(3))
	if !regexp.MustCompile(`GROUP BY val ORDER BY val asc +LIMIT 5`).MatchString(values) || !strings.Contains(values, "(key) == ('x')") {
		t.Errorf("values are not distinct values of x in order:\n%s", values)
	}
}

func TestAutocompleteSpanStartIsLeftOpenRightClosed(t *testing.T) {
	for _, query := range filteredQueries() {
		for kind, plan := range autocompletePlans(t, query) {
			for statement, p := range map[string]shared.SQLRequestPlanner{"search": plan.Ids[0].Statement, "data": plan.Data} {
				t.Run(kind+" "+statement+" "+query, func(t *testing.T) {
					res := renderPlan(t, p, testPlannerContext(3))
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
}

// The root filter applies on every evaluation of a plan.
func TestAutocompleteRootFilterAppliesToTheResultSpans(t *testing.T) {
	for kind, plan := range autocompletePlans(t, `{nestedSetParent < 0 && .a = 1}`) {
		for i := 0; i < 2; i++ {
			res := renderPlan(t, plan.Ids[0].Statement, testPlannerContext(3))
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
			_, err := plan.Ids[0].Statement.Process(old)
			if structural && err != errStructuralVersion {
				t.Errorf("%s %s: error = %v, want %v", kind, query, err, errStructuralVersion)
			}
			if !structural && err != nil {
				t.Errorf("%s %s: %v", kind, query, err)
			}
			if _, err := plan.Data.Process(old); err != nil {
				t.Errorf("%s %s: data: %v", kind, query, err)
			}
			res := renderPlan(t, plan.Ids[0].Statement, testPlannerContext(3))
			if strings.HasSuffix(res, " SETTINGS enable_analyzer=1") != structural ||
				strings.Count(res, "enable_analyzer") != map[bool]int{true: 1}[structural] {
				t.Errorf("%s %s: enable_analyzer pinned wrongly:\n%s", kind, query, res)
			}
		}
	}
}

// The search statement of a non-structural query reads Distributed tables on
// a cluster; TestStructuralIdsStatementsRunWholeOnEachShard covers the others.
func TestAutocompleteReadsDistributedTablesOnClusters(t *testing.T) {
	local := regexp.MustCompile(`\btempo_traces(_attrs_gin)?\b[^_]`)
	plainIn := regexp.MustCompile(`[^L] IN \((trace_ids|attrless_trace_ids| SELECT trace_id FROM \w+)\)`)
	for _, query := range filteredQueries() {
		if slices.Contains(structuralQueries, query) {
			continue
		}
		for kind, plan := range autocompletePlans(t, query) {
			t.Run(kind+" "+query, func(t *testing.T) {
				ctx := testPlannerContext(3)
				ctx.IsCluster = true
				res := renderPlan(t, plan.Ids[0].Statement, ctx)
				if m := local.FindString(res); m != "" {
					t.Errorf("search reads a local table (%q):\n%s", m, res)
				}
				if m := plainIn.FindString(res); m != "" {
					t.Errorf("distributed subquery under a plain IN (%q):\n%s", m, res)
				}
				if res := renderPlan(t, plan.Data, ctx); local.MatchString(res) {
					t.Errorf("data reads a local table:\n%s", res)
				}
			})
		}
	}
}
