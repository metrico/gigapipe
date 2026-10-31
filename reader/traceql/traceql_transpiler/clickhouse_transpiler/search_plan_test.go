package clickhouse_transpiler

import (
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

var plainSearchQueries = []string{
	`{.a = 1}`,
	`{}`,
	`{.a = 1} | count() > 1`,
	`{.a = 1} | avg(.b) > 1`,
	`{.a = 1} && {.b = 2}`,
	`{.a = 1} || {.b = 2}`,
	`{.a = 1} && {.b = 2} || {.c = 3}`,
	`({.a = 1} || {.b = 2}) && {.c = 3}`,
	`({.a = 1} | count() > 1) && {.b = 2}`,
	`count() > 1`,
	`{.a = 1} | count() > 1 | avg(duration) > 1ms | max(.b) < 3`,
	`{nestedSetParent < 0 && .a = 1}`,
	`{nestedSetParent < 0}`,
	`{nestedSetRight - nestedSetLeft = 1 && .a = 1}`,
	`{.a = 1} | by(nestedSetParent) | max(nestedSetLeft) > 1`,
	`{.a = .b}`,
	`{!(.a = 1) || duration > 100}`,
	`{duration * 2 > 1s}`,
	`{.a = 1} | avg(.b + .c) > 1`,
	`{.a = 1} | by(.b) | count() > 1 | coalesce()`,
	`by(.b * 2) | by(duration)`,
}

// oneHopQueries relate spans to their parent or siblings.
var oneHopQueries = []string{
	`{.a = 1} > {.b = 2}`,
	`{.a = 1} !~ {}`,
	`{} &< {.b = 2} || {.c = 3}`,
}

// transitiveQueries relate spans to their ancestors or descendants.
var transitiveQueries = []string{
	`{.a = 1} >> {.b = 2}`,
	`{.a = 1} !<< {}`,
	`{} &>> {.b = 2} && {.c = 3}`,
	`{.a = 1} &<< ({.b = 2} !>> {.c = 3})`,
	`{.a = 1} >> {.b = 2} | sum(.c) > 1`,
	`{.a = .b} >> {.c = nil}`,
}

// structuralQueries hold operators evaluated on the span tree.
var structuralQueries = append(append([]string{}, oneHopQueries...), transitiveQueries...)

var searchQueries = append(append([]string{}, plainSearchQueries...), structuralQueries...)

func testPlannerContext(spss int64) *shared.PlannerContext {
	return &shared.PlannerContext{
		From:                     time.Unix(86390, 0).UTC(),
		To:                       time.Unix(86410, 0).UTC(),
		Limit:                    5,
		SpansPerSpanSet:          spss,
		TracesAttrsTable:         "tempo_traces_attrs_gin",
		TracesAttrsDistTable:     "tempo_traces_attrs_gin_dist",
		TracesTable:              "tempo_traces",
		TracesDistTable:          "tempo_traces_dist",
		TracesNestedSetTable:     "tempo_traces_nested_set",
		TracesNestedSetDistTable: "tempo_traces_nested_set_dist",
		VersionInfo:              map[string]int64{dbversion.CapStructuralQueries: 0},
	}
}

func renderPlan(t *testing.T, planner shared.SQLRequestPlanner, ctx *shared.PlannerContext) string {
	t.Helper()
	req, err := planner.Process(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err := req.String(sql.DefaultCtx())
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func planSearch(t *testing.T, query string) shared.SQLRequestPlanner {
	t.Helper()
	q, err := traceql_ir.Parse(query)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := Plan(q)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestSearchOperandSetsAreUncapped(t *testing.T) {
	capped := regexp.MustCompile(`group(Uniq)?Array\(\d+\)`)
	for _, query := range searchQueries {
		t.Run(query, func(t *testing.T) {
			res := renderPlan(t, planSearch(t, query), testPlannerContext(7))
			if m := capped.FindString(res); m != "" {
				t.Errorf("operand set capped by %s:\n%s", m, res)
			}
		})
	}
}

// searchStages unwraps the planners above the evaluated expression.
func searchStages(t *testing.T, plan shared.SQLRequestPlanner) (stages []string, expr shared.SQLRequestPlanner) {
	t.Helper()
	for {
		switch p := plan.(type) {
		case *StructuralQueryPlanner:
			stages = append(stages, "structural")
			plan = p.Main
		case *IndexLimitPlanner:
			stages = append(stages, "limit")
			plan = p.Main
		case *TracesDataPlanner:
			stages = append(stages, "traces")
			plan = p.Main
		case *SearchSpansetPlanner:
			stages = append(stages, "spanset")
			return stages, p.Main
		default:
			t.Fatalf("unexpected planner %T above the search spanset stage", plan)
		}
	}
}

func TestSearchCapsAndLimitsAfterEvaluation(t *testing.T) {
	for _, query := range searchQueries {
		t.Run(query, func(t *testing.T) {
			stages, expr := searchStages(t, planSearch(t, query))
			want := []string{"limit", "traces", "limit", "spanset"}
			if slices.Contains(structuralQueries, query) {
				want = append([]string{"structural"}, want...)
			}
			if strings.Join(stages, ",") != strings.Join(want, ",") {
				t.Errorf("stages = %v, want %v", stages, want)
			}
			switch expr.(type) {
			case *IndexLimitPlanner, *TracesDataPlanner, *SearchSpansetPlanner:
				t.Errorf("expression planner is %T", expr)
			}
		})
	}
}

func TestSearchSpanCapComesFromSpss(t *testing.T) {
	for _, c := range []struct {
		spss int64
		want string
	}{{7, ", 1, 7)"}, {0, ", 1, 3)"}} {
		for _, query := range searchQueries {
			res := renderPlan(t, planSearch(t, query), testPlannerContext(c.spss))
			if n := strings.Count(res, "arraySlice("); n != 1 {
				t.Errorf("%s: %d span caps, want 1:\n%s", query, n, res)
			}
			if !strings.Contains(res, c.want) {
				t.Errorf("%s: spss %d not applied as %q:\n%s", query, c.spss, c.want, res)
			}
		}
	}
}

func TestSearchSpanCapKeepsEarliestStarts(t *testing.T) {
	const earliestCap = "arraySlice(arraySort(groupUniqArray((result_spans.timestamp_ns, result_spans.span_id))), 1, 3)"
	for _, query := range searchQueries {
		res := renderPlan(t, planSearch(t, query), testPlannerContext(3))
		if !strings.Contains(res, earliestCap) {
			t.Errorf("%s: span cap is not earliest start, ties by span id:\n%s", query, res)
		}
	}
}

func TestSearchRootFilterBeforeSpanCap(t *testing.T) {
	const query = `{nestedSetParent < 0 && .a = 1}`
	_, expr := searchStages(t, planSearch(t, query))
	res := renderPlan(t, &SearchSpansetPlanner{Main: expr}, testPlannerContext(3))
	if !strings.Contains(res, rootColumn) {
		t.Errorf("root filter missing from the spanset stage:\n%s", res)
	}
	res = renderPlan(t, planSearch(t, query), testPlannerContext(3))
	if n := strings.Count(res, "traces.parent_id"); n != 1 {
		t.Errorf("root filter applied %d times, want once:\n%s", n, res)
	}
}

// spanStartBounds lists the span start-time comparisons against the test
// range, which spans midnight of 1970-01-01.
func spanStartBounds(res string) []string {
	re := regexp.MustCompile(`timestamp_ns\) (>=|>|<=|<) \((86390|86410)000000000\)`)
	var out []string
	for _, m := range re.FindAllStringSubmatch(res, -1) {
		out = append(out, m[1]+m[2])
	}
	return out
}

func TestSearchSpanStartIsLeftOpenRightClosed(t *testing.T) {
	check := func(t *testing.T, res string) {
		t.Helper()
		bounds := spanStartBounds(res)
		if len(bounds) == 0 {
			t.Fatalf("no span start bounds:\n%s", res)
		}
		for _, b := range bounds {
			if b != ">86390" && b != "<=86410" {
				t.Errorf("span start bound %s, want >86390 or <=86410:\n%s", b, res)
			}
		}
		if strings.Contains(res, "(date)") &&
			(!strings.Contains(res, "(date) >= ('1970-01-01')") || !strings.Contains(res, "(date) <= ('1970-01-02')")) {
			t.Errorf("date pruning is not toDate(from)..toDate(to):\n%s", res)
		}
	}
	for _, query := range searchQueries {
		t.Run(query, func(t *testing.T) {
			check(t, renderPlan(t, planSearch(t, query), testPlannerContext(3)))
		})
	}
}

func TestSearchReadsDistributedTablesOnClusters(t *testing.T) {
	local := regexp.MustCompile(`\btempo_traces(_attrs_gin)?\b[^_]`)
	localJoin := regexp.MustCompile(`[^l] any left JOIN`)
	plainIn := regexp.MustCompile(`[^L] IN \((trace_ids|trace_span_ids|attrless_trace_ids| SELECT trace_id FROM \w+)\)`)
	for _, query := range searchQueries {
		t.Run(query, func(t *testing.T) {
			ctx := testPlannerContext(3)
			ctx.IsCluster = true
			res := renderPlan(t, planSearch(t, query), ctx)
			if m := local.FindString(res); m != "" {
				t.Errorf("reads a local table (%q):\n%s", m, res)
			}
			if m := plainIn.FindString(res); m != "" {
				t.Errorf("distributed subquery under a plain IN (%q):\n%s", m, res)
			}
			if localJoin.MatchString(res) {
				t.Errorf("distributed join is not GLOBAL:\n%s", res)
			}
		})
	}
}
