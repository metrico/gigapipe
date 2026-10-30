package clickhouse_transpiler

import (
	"regexp"
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
)

// narrowedRead matches an operand read restricted to the spans of a stage.
var narrowedRead = regexp.MustCompile(`\(trace_id, span_id\) (?:GLOBAL )?IN \(\s*SELECT trace_id, span_id FROM (_\d+stage\d+)\)`)

// narrowedReads counts the operand reads restricted to each stage's spans.
func narrowedReads(res string) map[string]int {
	out := map[string]int{}
	for _, m := range narrowedRead.FindAllStringSubmatch(res, -1) {
		out[m[1]]++
	}
	return out
}

// narrowingPlans plans query for search, metrics and autocomplete.
func narrowingPlans(t *testing.T, query string) map[string]shared.SQLRequestPlanner {
	t.Helper()
	plans := map[string]shared.SQLRequestPlanner{}
	for kind, plan := range autocompletePlans(t, query) {
		plans[kind] = plan.Ids[0].Statement
	}
	plans["search"] = planSearch(t, query).Ids[0].Statement
	plans["metrics"] = planMetrics(t, query).Ids[0].Statement
	return plans
}

// Every operand of a later spanset stage, nested pipelines included, reads
// only the spans of the stage before it; first-stage operands read every span.
func TestNarrowedStagesDrawEveryOperandFromThePreviousStage(t *testing.T) {
	for _, c := range []struct {
		query string
		// reads is the number of operand reads narrowed to stage i, by i.
		reads []int
	}{
		{`{.a = 1} | {.b = 2}`, []int{1}},
		{`{.a = 1} | {}`, []int{1}},
		{`{.a = 1} | {.b = 2} > {.c = 3}`, []int{2}},
		{`{.a = 1} | {.b = .c} >> {}`, []int{2}},
		{`{.a = 1} | {.b = 2} && ({.c = 3} || {.d = 4})`, []int{3}},
		{`{.a = 1} >> {.b = 2} | count() > 1 | {.c = 3} !~ {.d = 4}`, []int{0, 2}},
		{`{.a = 1} | ({.b = 2} | count() > 1) > {.c = 3}`, []int{2}},
		{`{.a = 1} | {.b = 2} | {.c = 3} &<< {}`, []int{1, 2}},
		{`count() > 1 | {.a = 1}`, []int{0, 1}},
	} {
		for kind, plan := range narrowingPlans(t, c.query) {
			t.Run(kind+" "+c.query, func(t *testing.T) {
				res := renderPlan(t, plan, testPlannerContext(3))
				got := narrowedReads(res)
				want := map[string]int{}
				for i, n := range c.reads {
					if n > 0 {
						want[stageCTEs(res)[i]] = n
					}
				}
				if len(got) != len(want) {
					t.Fatalf("narrowed reads %v, want %v:\n%s", got, want, res)
				}
				for stage, n := range want {
					if got[stage] != n {
						t.Errorf("narrowed reads %v, want %v:\n%s", got, want, res)
					}
				}
			})
		}
	}
}

// `{A} | {B}` is the B spans among A's result spans, not a trace-level `&&`.
func TestSpansetStageIsASpanLevelIntersection(t *testing.T) {
	_, expr := searchStages(t, planSearch(t, `{.a = 1} | {.b = 2}`))
	if got := describePlan(t, expr); got != `{.a}|{.b}` {
		t.Errorf("plan = %s", got)
	}
}

// A narrowed stage keeps the attributes and group keys of the spans it
// returns from the previous stage. A lone parenthesised pipeline starts from
// them and its rows carry its own.
func TestNarrowedStagesCarryThePreviousStageAttributes(t *testing.T) {
	for _, c := range []struct{ query, attrs string }{
		{`{.a = 1} | count() > 1 | {.b = 2}`, `_\d+stage1\.attrs as attrs, _\d+stage1\.grp as grp`},
		{`{.a = 1} | count() > 1 | ({.b = 2} | avg(.c) > 1)`,
			`_\d+stage0 as \( SELECT [^()]*_(\d+)stage1\.attrs as attrs, _\d+stage1\.grp as grp FROM _\d+stage0_expr inner JOIN _\d+stage1 ` +
				`.*_\d+stage2 as \( SELECT [^()]*_\d+stage2_expr\.attrs as attrs, _\d+stage2_expr\.grp as grp FROM _\d+stage2_expr\)`},
		{`({.b = 2} | avg(.c) > 1)`, `_\d+stage0_expr\.attrs as attrs, _\d+stage0_expr\.grp as grp FROM _\d+stage0_expr\)`},
	} {
		t.Run(c.query, func(t *testing.T) {
			res := renderSearch(t, planSearch(t, c.query), testPlannerContext(3))
			if !regexp.MustCompile(c.attrs).MatchString(res) {
				t.Errorf("attrs are not %s:\n%s", c.attrs, res)
			}
		})
	}
}

// Ancestry loads the trees of the traces whose narrowed operands return spans.
func TestNarrowedAncestryLoadsSurvivingTraces(t *testing.T) {
	const query = `{.a = 1} | {.b = 2} >> {.c = 3}`
	for _, cluster := range []bool{false, true} {
		ctx := testPlannerContext(3)
		ctx.IsCluster = cluster
		res := renderSearch(t, planSearch(t, query), ctx)
		edges := regexp.MustCompile(`(_\d+transitive_edges) as \(.*?GROUP BY`).FindString(res)
		operands := regexp.MustCompile(`IN \(\s*SELECT trace_id FROM (_\d+transitive_(?:lhs|rhs))\)`).FindAllStringSubmatch(edges, -1)
		if len(operands) != 2 {
			t.Fatalf("edges do not read the operand traces:\n%s", edges)
		}
		for _, m := range operands {
			body := res[strings.Index(res, m[1]+" as ("):]
			if loc := narrowedRead.FindStringSubmatchIndex(body); loc == nil || strings.Contains(body[:loc[0]], "_edges as (") {
				t.Errorf("%s is not narrowed to the previous stage:\n%s", m[1], res)
			}
		}
	}
}
