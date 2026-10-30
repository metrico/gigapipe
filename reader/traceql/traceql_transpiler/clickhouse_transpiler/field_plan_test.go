package clickhouse_transpiler

import (
	"regexp"
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
)

// selectorPlan plans query, a lone selector, and returns the planner of its span rows.
func selectorPlan(t *testing.T, query string) shared.SQLRequestPlanner {
	t.Helper()
	_, expr := searchStages(t, planSearch(t, query))
	return expr.(*pipelinePlanner).Stages[0].(*spansetStage).Expr
}

var pivotKeys = regexp.MustCompile(`traces_idx\.key IN \(([^)]*)\)`)

// A boolean combination of `attribute op literal` reads the attribute index
// as a bitset of its comparisons; any other filter pivots the attributes it reads.
func TestFilterPlansBitsetOrPivot(t *testing.T) {
	bitset := []string{
		`{.a = 1}`,
		`{.a = "x" && (.b != 2 || .c =~ "y")}`,
		`{span.a >= 1.5 || resource.b < -3}`,
		`{duration > 1ms && name = "x"}`,
		`{status = error || kind != server}`,
		`{statusMessage = "x" && rootServiceName = "y" && rootName = "z"}`,
		`{span.foo = bar}`,
		`{true}`,
	}
	pivot := []string{
		`{.a = .b}`,
		`{.a + 1 > 2}`,
		`{-.a = 1}`,
		`{!(.a = 1)}`,
		`{!.a}`,
		`{.a}`,
		`{.a = nil}`,
		`{.a != nil}`,
		`{.a = true}`,
		`{.a > 5ms}`,
		`{.a > "x"}`,
		`{1 < .a}`,
		`{duration > 100}`,
		`{name =~ "x" && .a = .b}`,
		`{nestedSetParent < 0 && .a = 1}`,
		`{nestedSetLeft > 3}`,
	}
	for _, query := range bitset {
		t.Run(query, func(t *testing.T) {
			plan := selectorPlan(t, query)
			if _, ok := plan.(*AttrConditionPlanner); !ok {
				t.Fatalf("planned by %T", plan)
			}
			res := renderSearch(t, planSearch(t, query), testPlannerContext(3))
			if !strings.Contains(res, "groupBitOr(") || strings.Contains(res, "maxIfOrNull(") {
				t.Errorf("not a bitset plan:\n%s", res)
			}
		})
	}
	for _, query := range pivot {
		t.Run(query, func(t *testing.T) {
			plan := selectorPlan(t, query)
			if _, ok := plan.(*FieldFilterPlanner); !ok {
				t.Fatalf("planned by %T", plan)
			}
			res := renderSearch(t, planSearch(t, query), testPlannerContext(3))
			if strings.Contains(res, "groupBitOr(") {
				t.Errorf("not a pivot plan:\n%s", res)
			}
		})
	}
}

// The pivot reads the keys the filter reads, plus the key every span has when
// the filter can hold for spans lacking all of them; a filter of no key reads
// the traces table.
func TestPivotReadsTheFilterKeys(t *testing.T) {
	for _, c := range []struct{ query, keys string }{
		{`{.a = .b}`, `'a', 'b'`},
		{`{span.a + resource.b > .a}`, `'a', 'b'`},
		{`{.a != nil}`, `'a'`},
		{`{.a && .b = .c}`, `'a', 'b', 'c'`},
		{`{.a = nil}`, `'a', 'name'`},
		{`{!(.a = 1)}`, `'a', 'name'`},
		{`{!.a}`, `'a'`},
		{`{.a = .b || duration > 100}`, `'a', 'b', 'name'`},
		{`{.a = .b || false}`, `'a', 'b'`},
		{`{name = .a}`, `'name', 'a'`},
		{`{rootServiceName = .a && status = error}`, `'service.name', 'a', 'status'`},
		{`{duration > 100}`, ``},
		{`{!(duration > 1s)}`, ``},
	} {
		t.Run(c.query, func(t *testing.T) {
			res := renderSearch(t, planSearch(t, c.query), testPlannerContext(3))
			pivot := res[strings.Index(res, "_pivot as ("):]
			pivot = pivot[:strings.Index(pivot, "GROUP BY")]
			if c.keys == "" {
				if !strings.Contains(pivot, "FROM tempo_traces as traces") || strings.Contains(pivot, "traces_idx") {
					t.Errorf("does not read the traces table:\n%s", pivot)
				}
				return
			}
			m := pivotKeys.FindStringSubmatch(pivot)
			if m == nil || m[1] != c.keys {
				t.Errorf("pivot keys %v, want %s:\n%s", m, c.keys, pivot)
			}
			if n := strings.Count(pivot, "maxIfOrNull("); n != strings.Count(c.keys, ",")+1 {
				t.Errorf("%d pivot columns for keys %s:\n%s", n, c.keys, pivot)
			}
		})
	}
}

// Aggregate arguments read the pivot of the keys they reference, or the
// traces table for duration alone.
func TestAggregateArgumentsPivot(t *testing.T) {
	for _, c := range []struct{ query, keys string }{
		{`{.a = 1} | avg(.b + .c) > 1`, `'b', 'c'`},
		{`{.a = 1} | sum(-span.b * duration) > 1`, `'b'`},
		{`{.a = 1} | max(spanN) > 1`, `'spanN'`},
		{`{.a = 1} | min(duration * 2) > 1ms`, ``},
	} {
		t.Run(c.query, func(t *testing.T) {
			res := renderSearch(t, planSearch(t, c.query), testPlannerContext(3))
			m := regexp.MustCompile(`_\d+stage1_pivot as \((.*?)GROUP BY`).FindStringSubmatch(res)
			if m == nil {
				t.Fatalf("no value pivot:\n%s", res)
			}
			if c.keys == "" {
				if !strings.Contains(m[1], "FROM tempo_traces as traces") {
					t.Errorf("does not read the traces table:\n%s", m[1])
				}
				return
			}
			if k := pivotKeys.FindStringSubmatch(m[1]); k == nil || k[1] != c.keys {
				t.Errorf("value pivot keys %v, want %s:\n%s", k, c.keys, m[1])
			}
		})
	}
}

// The bitset reads the rows of every term that reads a key, and every row
// when the filter can hold without any of them.
func TestBitsetReadsTheRowsOfItsKeyTerms(t *testing.T) {
	where := regexp.MustCompile(`_\d+stage0_expr as \( SELECT .*? WHERE (.*?) GROUP BY`)
	for _, c := range []struct {
		query string
		reads []string
	}{
		{`{status = error && .a = 1}`, []string{`('status')`, `('a')`}},
		{`{kind = server || .a = "x"}`, []string{`('kind')`, `('a')`}},
		{`{rootServiceName = "x" && .a = 1}`, []string{`('service.name')`, `('a')`}},
		{`{duration > 1ms && .a = 1}`, []string{`('a')`}},
		{`{duration > 1ms || .a = 1}`, nil},
		{`{true || .a = 1}`, nil},
	} {
		t.Run(c.query, func(t *testing.T) {
			res := renderSearch(t, planSearch(t, c.query), testPlannerContext(3))
			m := where.FindStringSubmatch(res)
			if m == nil {
				t.Fatalf("no bitset stage:\n%s", res)
			}
			keyed := strings.Contains(m[1], "(key) == ")
			if keyed != (c.reads != nil) {
				t.Errorf("reads rows by key = %v:\n%s", keyed, m[1])
			}
			for _, key := range c.reads {
				if !strings.Contains(m[1], "(key) == "+key) {
					t.Errorf("does not read the rows of %s:\n%s", key, m[1])
				}
			}
		})
	}
}

// Field-expression errors surface when planning search, metrics and autocomplete.
func TestFieldExpressionErrors(t *testing.T) {
	for _, c := range []struct{ query, err string }{
		{`{.a + 1}`, "span filter field expressions must resolve to a boolean: { .a + 1 }"},
		{`{status = "error"}`, `binary operations must operate on the same type: status = "error"`},
		{`{kind > server}`, "illegal operation for the given types: kind > server"},
		{`{.a = .b && span:id = "x"}`, "unsupported attribute span:id"},
		{`{} | avg(.a > 1) > 1`, "aggregate field expressions must resolve to a number type: avg(.a > 1)"},
		{`{} | avg(2) > 1`, "aggregate field expressions must reference the span: avg(2)"},
	} {
		t.Run(c.query, func(t *testing.T) {
			q := mustParseQuery(t, c.query)
			search, err := Plan(q)
			if err == nil {
				_, err = search.Search.Process(testPlannerContext(3))
			}
			if err == nil || err.Error() != c.err {
				t.Errorf("search error = %v, want %s", err, c.err)
			}
			metrics, err := PlanMetrics(q, countSpans)
			if err == nil {
				err = processMetrics(metrics, testPlannerContext(3))
			}
			if err == nil || err.Error() != c.err {
				t.Errorf("metrics error = %v, want %s", err, c.err)
			}
			plan, err := PlanTagsV2(q)
			if err == nil {
				_, err = plan.Process(testPlannerContext(3))
			}
			if err == nil || err.Error() != c.err {
				t.Errorf("tags error = %v, want %s", err, c.err)
			}
		})
	}
}

func mustParseQuery(t *testing.T, query string) *traceql_ir.Query {
	t.Helper()
	q, err := traceql_ir.Parse(query)
	if err != nil {
		t.Fatal(err)
	}
	return q
}
