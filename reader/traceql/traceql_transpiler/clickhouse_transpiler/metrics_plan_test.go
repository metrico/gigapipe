package clickhouse_transpiler

import (
	"fmt"
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

func planMetrics(t *testing.T, query string) *MetricsPlan {
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

// metricsResult unwraps a metrics plan's ids statement down to its evaluated
// expression.
func metricsResult(t *testing.T, plan *MetricsPlan) (structural bool, expr shared.SQLRequestPlanner) {
	t.Helper()
	if len(plan.Ids) != 1 {
		t.Fatalf("metrics plan has %d ids statements", len(plan.Ids))
	}
	return idsResult(t, plan.Ids[0].Statement)
}

// idsResult unwraps an ids statement down to its evaluated expression.
func idsResult(t *testing.T, statement shared.SQLRequestPlanner) (structural bool, expr shared.SQLRequestPlanner) {
	t.Helper()
	if s, ok := statement.(*StructuralQueryPlanner); ok {
		structural = true
		statement = s.Main
	}
	if s, ok := statement.(*shardLocalPlanner); ok {
		statement = s.Single
	}
	if w, ok := statement.(*windowPlanner); ok {
		statement = w.Main
	}
	ids, ok := statement.(*spanIdsPlanner)
	if !ok {
		t.Fatalf("ids statement is %T", statement)
	}
	return structural, ids.Result
}

// renderMetrics renders plan's ids statements and its metrics statement, in
// that order.
func renderMetrics(t *testing.T, plan *MetricsPlan, ctx *shared.PlannerContext) string {
	t.Helper()
	var res []string
	for _, ids := range plan.Ids {
		res = append(res, renderPlan(t, ids.Statement, ctx))
	}
	return strings.Join(append(res, renderPlan(t, plan.Metrics, ctx)), ";\n")
}

// processMetrics processes plan's statements, returning the first error.
func processMetrics(plan *MetricsPlan, ctx *shared.PlannerContext) error {
	for _, ids := range plan.Ids {
		if _, err := ids.Statement.Process(ctx); err != nil {
			return err
		}
	}
	_, err := plan.Metrics.Process(ctx)
	return err
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
			res := renderMetrics(t, planMetrics(t, query), testPlannerContext(3))
			if m := capped.FindString(res); m != "" {
				t.Errorf("metrics spans capped by %s:\n%s", m, res)
			}
		})
	}
}

func TestMetricsSpansJoinResultSpansOnTraceAndSpan(t *testing.T) {
	for _, query := range searchQueries {
		t.Run(query, func(t *testing.T) {
			res := renderPlan(t, planMetrics(t, query).Metrics, testPlannerContext(3))
			if !regexp.MustCompile(`\(traces\.trace_id, traces\.span_id\) IN \(_\d+trace_span_ids\)`).MatchString(res) {
				t.Errorf("metrics spans are not the result spans by (trace_id, span_id):\n%s", res)
			}
		})
	}
}

func TestMetricsSpanStartIsLeftOpenRightClosed(t *testing.T) {
	for _, query := range searchQueries {
		t.Run(query, func(t *testing.T) {
			res := renderMetrics(t, planMetrics(t, query), testPlannerContext(3))
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
	res := renderMetrics(t, planMetrics(t, `{nestedSetParent < 0 && .a = 1}`), testPlannerContext(3))
	if !strings.Contains(res, rootColumn) {
		t.Errorf("root filter missing from the metrics spans:\n%s", res)
	}
}

func TestMetricsStructuralQueriesNeedClickHouse249AndTheAnalyzer(t *testing.T) {
	old := testPlannerContext(3)
	old.VersionInfo = map[string]int64{}
	for _, query := range searchQueries {
		structural := slices.Contains(structuralQueries, query)
		err := processMetrics(planMetrics(t, query), old)
		if structural && err != errStructuralVersion {
			t.Errorf("%s: error = %v, want %v", query, err, errStructuralVersion)
		}
		if !structural && err != nil {
			t.Errorf("%s: %v", query, err)
		}
		plan := planMetrics(t, query)
		res := renderPlan(t, plan.Ids[0].Statement, testPlannerContext(3))
		if strings.HasSuffix(res, " SETTINGS enable_analyzer=1") != structural ||
			strings.Count(res, "enable_analyzer") != map[bool]int{true: 1}[structural] {
			t.Errorf("%s: enable_analyzer pinned wrongly on the ids statement:\n%s", query, res)
		}
		if res := renderPlan(t, plan.Metrics, testPlannerContext(3)); strings.Contains(res, "enable_analyzer") {
			t.Errorf("%s: enable_analyzer on the metrics statement:\n%s", query, res)
		}
	}
}

// The ids statement of a non-structural query reads Distributed tables on a
// cluster; TestStructuralIdsStatementsRunWholeOnEachShard covers the others.
func TestMetricsReadDistributedTablesOnClusters(t *testing.T) {
	local := regexp.MustCompile(`\btempo_traces(_attrs_gin|_nested_set)?\b[^_]`)
	plainIn := regexp.MustCompile(`[^L] IN \((trace_ids|attrless_trace_ids| SELECT trace_id FROM \w+)\)`)
	for _, query := range plainSearchQueries {
		t.Run(query, func(t *testing.T) {
			ctx := testPlannerContext(3)
			ctx.IsCluster = true
			plan := planMetrics(t, query)
			res := renderPlan(t, plan.Ids[0].Statement, ctx)
			if m := local.FindString(res); m != "" {
				t.Errorf("ids statement reads a local table (%q):\n%s", m, res)
			}
			if m := plainIn.FindString(res); m != "" {
				t.Errorf("distributed subquery under a plain IN (%q):\n%s", m, res)
			}
			if res := renderPlan(t, plan.Metrics, ctx); local.MatchString(res) {
				t.Errorf("metrics statement reads a local table:\n%s", res)
			}
		})
	}
}

var metricsQueries = append(append([]string{}, searchQueries...),
	`{.a = 1} | avg_over_time(nestedSetRight) by (nestedSetLeft)`,
)

func planMetricsFn(t *testing.T, query string) *MetricsPlan {
	t.Helper()
	if strings.Contains(query, "_over_time") {
		q, err := traceql_ir.Parse(query)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := PlanMetrics(q, countSpans)
		if err != nil {
			t.Fatal(err)
		}
		return plan
	}
	return planMetrics(t, query)
}

// The ids statement lists the result span ids by trace into SearchResultTable.
func TestMetricsIdsStatementListsSpanIdsByTrace(t *testing.T) {
	for _, query := range metricsQueries {
		for _, cluster := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cluster=%t", query, cluster), func(t *testing.T) {
				ctx := testPlannerContext(3)
				ctx.IsCluster = cluster
				plan := planMetricsFn(t, query)
				if structural, _ := metricsResult(t, plan); structural && cluster {
					t.Skip("on a cluster TestStructuralIdsStatementsRunWholeOnEachShard covers it")
				}
				if plan.Ids[0].Table != SearchResultTable {
					t.Errorf("ids table %s, want %s", plan.Ids[0].Table, SearchResultTable)
				}
				res := renderPlan(t, plan.Ids[0].Statement, ctx)
				if !strings.HasPrefix(res, "WITH") ||
					!regexp.MustCompile(`SELECT trace_id, groupArray\(span_id\) as span_ids FROM \w+ GROUP BY trace_id( SETTINGS enable_analyzer=1)?$`).MatchString(res) {
					t.Errorf("ids statement is not (trace_id, groupArray(span_id)) by trace:\n%s", res)
				}
				if strings.Contains(res, SearchResultTable) {
					t.Errorf("ids statement reads %s:\n%s", SearchResultTable, res)
				}
			})
		}
	}
}

// The metrics statement reads the result spans of SearchResultTable alone,
// with plain IN on a cluster.
func TestMetricsStatementReadsOnlySearchResult(t *testing.T) {
	plainInRe := regexp.MustCompile(`[^L] IN \(_\d+(trace_ids|trace_span_ids)\)`)
	for _, query := range metricsQueries {
		for _, cluster := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cluster=%t", query, cluster), func(t *testing.T) {
				ctx := testPlannerContext(3)
				ctx.IsCluster = cluster
				res := renderPlan(t, planMetricsFn(t, query).Metrics, ctx)
				if !strings.Contains(res, "FROM "+SearchResultTable) {
					t.Errorf("does not read %s:\n%s", SearchResultTable, res)
				}
				for _, banned := range []string{"result_spans", "spansets", "index_grouped", "GLOBAL", "cityHash64"} {
					if strings.Contains(res, banned) {
						t.Errorf("reads %s:\n%s", banned, res)
					}
				}
				if !plainInRe.MatchString(res) {
					t.Errorf("no plain IN over the search result:\n%s", res)
				}
				if cluster && !strings.Contains(res, "tempo_traces_dist") {
					t.Errorf("does not read the distributed spans table:\n%s", res)
				}
			})
		}
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
	if len(plan.Ids) != 2 || plan.Ids[0].Table != SearchResultTable || plan.Ids[1].Table != selectionResultTable {
		t.Fatalf("compare ids statements %+v, want %s and %s", plan.Ids, SearchResultTable, selectionResultTable)
	}
	structural, expr := idsResult(t, plan.Ids[0].Statement)
	if got := describePlan(t, expr); !structural || got != ">>({.a},{.b})" {
		t.Errorf("baseline plan %s, structural %v", got, structural)
	}
	structural, expr = idsResult(t, plan.Ids[1].Statement)
	if got := describePlan(t, expr); structural || got != "{.c,.d}" {
		t.Errorf("selection plan %s, structural %v", got, structural)
	}
	ctx := testPlannerContext(3)
	baselineIds := renderPlan(t, plan.Ids[0].Statement, ctx)
	if !strings.Contains(baselineIds, "86000000000000") || strings.Contains(baselineIds, "86390000000000") {
		t.Errorf("baseline ids are not over the baseline window:\n%s", baselineIds)
	}
	selectionIds := renderPlan(t, plan.Ids[1].Statement, ctx)
	if !strings.Contains(selectionIds, "86390000000000") || strings.Contains(selectionIds, "86000000000000") {
		t.Errorf("selection ids are not over the query range:\n%s", selectionIds)
	}

	req, err := plan.Metrics.Process(ctx)
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
	if got := idsTableOf(t, withs, baseline); got != SearchResultTable {
		t.Errorf("baseline spans read %s, want %s", got, SearchResultTable)
	}
	selection := renderWith(t, withs["selection_spans"])
	if !strings.Contains(selection, "(traces.timestamp_ns) > (86390000000000)") ||
		!strings.Contains(selection, "(traces.timestamp_ns) <= (86410000000000)") {
		t.Errorf("selection spans are not over the query range:\n%s", selection)
	}
	if got := idsTableOf(t, withs, selection); got != selectionResultTable {
		t.Errorf("selection spans read %s, want %s", got, selectionResultTable)
	}
	for _, cluster := range []bool{false, true} {
		ctx.IsCluster = cluster
		res := renderPlan(t, plan.Metrics, ctx)
		for _, banned := range []string{"result_spans", "spansets", "GLOBAL", "enable_analyzer"} {
			if strings.Contains(res, banned) {
				t.Errorf("cluster %v: compare statement has %s:\n%s", cluster, banned, res)
			}
		}
	}

	q, err = traceql_ir.Parse(`{.a = 1} | compare({})`)
	if err != nil {
		t.Fatal(err)
	}
	plan, err = PlanCompare(q, q.Metrics.Compare.Filter, baselineFrom, baselineTo, compareSpans)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Ids) != 1 {
		t.Errorf("compare({}) plans %d ids statements, want 1", len(plan.Ids))
	}
	if res := renderPlan(t, plan.Metrics, testPlannerContext(3)); strings.Contains(res, selectionResultTable) {
		t.Errorf("compare({}) reads a selection:\n%s", res)
	}
}

// idsTableOf is the ids table behind the trace_ids CTE that spans, a rendered
// spans CTE, reads.
func idsTableOf(t *testing.T, withs map[string]*sql.With, spans string) string {
	t.Helper()
	m := regexp.MustCompile(`traces\.trace_id IN \((_\d+)trace_ids\)`).FindStringSubmatch(spans)
	if m == nil {
		t.Fatalf("spans read no trace_ids:\n%s", spans)
	}
	ids := renderWith(t, withs[m[1]+"result_ids"])
	table := regexp.MustCompile(`FROM (\w+)\)?$`).FindStringSubmatch(strings.TrimSpace(ids))
	if table == nil {
		t.Fatalf("result_ids read no table:\n%s", ids)
	}
	return table[1]
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
		res := regexp.MustCompile(`\b_\d+`).ReplaceAllString(renderPlan(t, plan.Metrics, ctx), "_N")
		table, in := "tempo_traces_nested_set as ns", "(ns.trace_id IN (_Ntrace_ids))"
		if cluster {
			table = "tempo_traces_nested_set_dist as ns"
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

// A metrics statement over a plan's ids with the plan's build is the plan's
// metrics statement.
func TestMetricsOverReadsThePlansIds(t *testing.T) {
	for _, query := range metricsQueries {
		t.Run(query, func(t *testing.T) {
			plan := planMetricsFn(t, query)
			over, err := plan.Over(countSpans)
			if err != nil {
				t.Fatal(err)
			}
			ctx := testPlannerContext(3)
			if got, exp := renderPlan(t, over, ctx), renderPlan(t, plan.Metrics, ctx); got != exp {
				t.Errorf("got\n%s\nwant\n%s", got, exp)
			}
		})
	}
}

func TestCompareHasNoMetricsOver(t *testing.T) {
	q, err := traceql_ir.Parse(`{} | compare({.a = "1"})`)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := PlanCompare(q, q.Metrics.Compare.Filter, time.Unix(0, 0), time.Unix(1, 0), compareSpans)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plan.Over(countSpans); err == nil {
		t.Error("compare plan has a metrics statement over its ids")
	}
}
