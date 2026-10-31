package clickhouse_transpiler

import (
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// countSpans is a metrics build that counts its spans.
func countSpans(spans *sql.With) (sql.ISelect, error) {
	return sql.NewSelect().
		With(spans).
		Select(sql.NewSimpleCol("count()", "value")).
		From(sql.NewWithRef(spans)), nil
}

func planMetrics(t *testing.T, query string) shared.SQLRequestPlanner {
	t.Helper()
	q, err := traceql_ir.Parse(query + " | rate()")
	if err != nil {
		t.Fatal(err)
	}
	if q.Metrics == nil {
		t.Fatalf("%s | rate() has no metrics function", query)
	}
	plan, err := PlanMetrics(q, countSpans)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

// metricsResult unwraps a metrics plan down to its evaluated expression.
func metricsResult(t *testing.T, plan shared.SQLRequestPlanner) (structural bool, expr shared.SQLRequestPlanner) {
	t.Helper()
	if s, ok := plan.(*StructuralQueryPlanner); ok {
		structural = true
		plan = s.Main
	}
	m, ok := plan.(*metricsPlanner)
	if !ok {
		t.Fatalf("metrics plan is %T", plan)
	}
	return structural, m.Spans.Result
}

// Metrics count the spans search returns: the same evaluated expression.
func TestMetricsReadTheSearchResultSpans(t *testing.T) {
	for _, query := range searchQueries {
		if query == `{}` {
			continue
		}
		t.Run(query, func(t *testing.T) {
			structural, expr := metricsResult(t, planMetrics(t, query))
			_, searchExpr := searchStages(t, planSearch(t, query))
			if got, want := describePlan(t, expr), describePlan(t, searchExpr); got != want {
				t.Errorf("metrics plan %s, search plan %s", got, want)
			}
			if structural != slices.Contains(structuralQueries, query) {
				t.Errorf("structural gate = %v", structural)
			}
		})
	}
	_, expr := metricsResult(t, planMetrics(t, `{}`))
	if got := describePlan(t, expr); got != "{}" {
		t.Errorf("{} | rate() reads %s, want every span in range", got)
	}
}

func TestMetricsSpansAreUncapped(t *testing.T) {
	capped := regexp.MustCompile(`group(Uniq)?Array\(\d+\)|arraySlice\(|LIMIT`)
	for _, query := range searchQueries {
		t.Run(query, func(t *testing.T) {
			res := renderPlan(t, planMetrics(t, query), testPlannerContext(3))
			if m := capped.FindString(res); m != "" {
				t.Errorf("metrics spans capped by %s:\n%s", m, res)
			}
		})
	}
}

func TestMetricsSpansJoinResultSpansOnTraceAndSpan(t *testing.T) {
	for _, query := range searchQueries {
		t.Run(query, func(t *testing.T) {
			res := renderPlan(t, planMetrics(t, query), testPlannerContext(3))
			if !regexp.MustCompile(`\(traces\.trace_id, traces\.span_id\) IN \(_\d+result_span_ids\)`).MatchString(res) {
				t.Errorf("metrics spans are not the result spans by (trace_id, span_id):\n%s", res)
			}
		})
	}
}

func TestMetricsSpanStartIsLeftOpenRightClosed(t *testing.T) {
	for _, query := range searchQueries {
		t.Run(query, func(t *testing.T) {
			res := renderPlan(t, planMetrics(t, query), testPlannerContext(3))
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

func TestMetricsRootFilterAppliesToTheResultSpans(t *testing.T) {
	res := renderPlan(t, planMetrics(t, `{nestedSetParent < 0 && .a = 1}`), testPlannerContext(3))
	if !strings.Contains(res, rootColumn) {
		t.Errorf("root filter missing from the metrics spans:\n%s", res)
	}
}

func TestMetricsStructuralQueriesNeedClickHouse249AndTheAnalyzer(t *testing.T) {
	old := testPlannerContext(3)
	old.VersionInfo = map[string]int64{}
	for _, query := range searchQueries {
		structural := slices.Contains(structuralQueries, query)
		_, err := planMetrics(t, query).Process(old)
		if structural && err != errStructuralVersion {
			t.Errorf("%s: error = %v, want %v", query, err, errStructuralVersion)
		}
		if !structural && err != nil {
			t.Errorf("%s: %v", query, err)
		}
		res := renderPlan(t, planMetrics(t, query), testPlannerContext(3))
		if strings.HasSuffix(res, " SETTINGS enable_analyzer=1") != structural ||
			strings.Count(res, "enable_analyzer") != map[bool]int{true: 1}[structural] {
			t.Errorf("%s: enable_analyzer pinned wrongly:\n%s", query, res)
		}
	}
}

func TestMetricsReadDistributedTablesOnClusters(t *testing.T) {
	local := regexp.MustCompile(`\btempo_traces(_attrs_gin)?\b[^_]`)
	plainIn := regexp.MustCompile(`[^L] IN \((trace_ids|\w*result_span_ids|attrless_trace_ids| SELECT trace_id FROM \w+)\)`)
	for _, query := range searchQueries {
		t.Run(query, func(t *testing.T) {
			ctx := testPlannerContext(3)
			ctx.IsCluster = true
			res := renderPlan(t, planMetrics(t, query), ctx)
			if m := local.FindString(res); m != "" {
				t.Errorf("reads a local table (%q):\n%s", m, res)
			}
			if m := plainIn.FindString(res); m != "" {
				t.Errorf("distributed subquery under a plain IN (%q):\n%s", m, res)
			}
		})
	}
}

// compareSpans is a compare build that counts baseline and selection spans.
func compareSpans(baseline, selection *sql.With) (sql.ISelect, error) {
	res := sql.NewSelect().
		With(baseline).
		Select(sql.NewSimpleCol("count()", "value")).
		From(sql.NewWithRef(baseline))
	if selection != nil {
		res.AddWith(selection).AndWhere(sql.NewIn(sql.NewRawObject("(trace_id, span_id)"),
			sql.NewSelect().Select(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id")).
				From(sql.NewWithRef(selection))))
	}
	return res, nil
}

func TestCompareReadsBaselineAndSelectionSpans(t *testing.T) {
	q, err := traceql_ir.Parse(`({.a = 1} >> {.b = 2}) | compare({.c = 3 || .d = 4})`)
	if err != nil {
		t.Fatal(err)
	}
	baselineFrom, baselineTo := time.Unix(86000, 0), time.Unix(86500, 0)
	plan, err := PlanCompare(q, q.Metrics.Compare.Filter, baselineFrom, baselineTo, compareSpans)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := plan.(*StructuralQueryPlanner); !ok {
		t.Fatalf("structural compare plan is %T", plan)
	}
	req, err := plan.Process(testPlannerContext(3))
	if err != nil {
		t.Fatal(err)
	}
	withs := map[string]*sql.With{}
	for _, w := range req.GetWith() {
		withs[w.GetAlias()] = w
	}
	baseline := renderWith(t, withs["baseline_spans"])
	if !strings.Contains(baseline, "(traces.timestamp_ns) > (86000000000000)") ||
		!strings.Contains(baseline, "(traces.timestamp_ns) <= (86500000000000)") {
		t.Errorf("baseline spans are not over the baseline window:\n%s", baseline)
	}
	selection := renderWith(t, withs["selection_spans"])
	if !strings.Contains(selection, "(traces.timestamp_ns) > (86390000000000)") ||
		!strings.Contains(selection, "(traces.timestamp_ns) <= (86410000000000)") {
		t.Errorf("selection spans are not over the query range:\n%s", selection)
	}
	plain := plan.(*StructuralQueryPlanner).Main.(*comparePlanner)
	if got := describePlan(t, plain.Baseline.Result); got != ">>({.a},{.b})" {
		t.Errorf("baseline plan %s", got)
	}
	if got := describePlan(t, plain.Selection.Result); got != "{.c,.d}" {
		t.Errorf("selection plan %s", got)
	}

	q, err = traceql_ir.Parse(`{.a = 1} | compare({})`)
	if err != nil {
		t.Fatal(err)
	}
	plan, err = PlanCompare(q, q.Metrics.Compare.Filter, baselineFrom, baselineTo, compareSpans)
	if err != nil {
		t.Fatal(err)
	}
	if plan.(*comparePlanner).Selection != nil {
		t.Errorf("compare({}) plans a selection")
	}
}

// Metrics read the stored nested-set values of the result spans, from the
// distributed table on a cluster.
func TestMetricsReadNestedSetValuesOfTheResultSpans(t *testing.T) {
	q, err := traceql_ir.Parse(`{.a = 1} | avg_over_time(nestedSetRight) by (nestedSetLeft, nestedSetRight)`)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := PlanMetrics(q, countSpans)
	if err != nil {
		t.Fatal(err)
	}
	for _, cluster := range []bool{false, true} {
		ctx := testPlannerContext(3)
		ctx.IsCluster = cluster
		res := regexp.MustCompile(`\b_\d+`).ReplaceAllString(renderPlan(t, plan, ctx), "_N")
		table, in := "tempo_traces_nested_set as ns", "(ns.trace_id IN ( SELECT trace_id FROM _Nresult_span_ids))"
		if cluster {
			table, in = "tempo_traces_nested_set_dist as ns", "(ns.trace_id GLOBAL IN ( SELECT trace_id FROM _Nresult_span_ids))"
		}
		for _, want := range []string{
			table, in,
			"argMax(ns.right, ns.version) as right_, argMax(ns.left, ns.version) as left_",
			"ifNull(_Nmetrics_nested_set.right_, 0) as nested_set_right, ifNull(_Nmetrics_nested_set.left_, 0) as nested_set_left",
		} {
			if !strings.Contains(res, want) {
				t.Errorf("cluster %v: %q missing:\n%s", cluster, want, res)
			}
		}
	}
}
