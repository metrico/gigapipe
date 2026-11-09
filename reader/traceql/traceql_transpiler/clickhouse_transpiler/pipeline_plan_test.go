package clickhouse_transpiler

import (
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
)

var stageCTE = regexp.MustCompile(`\b(_\d+stage\d+) as \(`)

// stageCTEs lists the pipeline stage CTEs of a rendered plan, in order.
func stageCTEs(res string) []string {
	var out []string
	for _, m := range stageCTE.FindAllStringSubmatch(res, -1) {
		out = append(out, m[1])
	}
	return out
}

func TestPipelinePlansOneCTEPerStage(t *testing.T) {
	for _, c := range []struct {
		query  string
		stages int
	}{
		{`{.a = 1}`, 1},
		{`{.a = 1} >> {.b = 2}`, 1},
		{`{.a = 1} | count() > 1`, 2},
		{`{.a = 1} > {.b = 2} | count() > 1 | avg(duration) > 1ms | max(.c) < 3`, 4},
		// A leading scalar filter reads every span in range first.
		{`count() > 1`, 2},
		{`count() > 1 | sum(.a) > 2`, 3},
	} {
		t.Run(c.query, func(t *testing.T) {
			for kind, plan := range map[string]shared.SQLRequestPlanner{
				"search":  planSearch(t, c.query),
				"metrics": planMetrics(t, c.query),
			} {
				res := renderPlan(t, plan, testPlannerContext(3))
				stages := stageCTEs(res)
				if len(stages) != c.stages {
					t.Fatalf("%s: stage CTEs %v, want %d:\n%s", kind, stages, c.stages, res)
				}
				// Stage i and its helper CTEs sit between stage i-1 and stage i.
				for i := 1; i < len(stages); i++ {
					body := res[strings.Index(res, stages[i-1]+" as ("):strings.Index(res, stages[i]+" as (")]
					body = body[len(stages[i-1]):]
					if !regexp.MustCompile(`FROM ` + stages[i-1] + `\b[^_]`).MatchString(body) {
						t.Errorf("%s: %s does not read %s:\n%s", kind, stages[i], stages[i-1], res)
					}
				}
			}
		})
	}
}

// A parenthesised pipeline is an operand with stages of its own.
func TestPipelinePlansParenthesisedPipelinesAsOperands(t *testing.T) {
	for _, c := range []struct{ query, want string }{
		{`({.a = 1} | count() > 1) >> {.b = 2}`, `>>({.a}|count() > 1,{.b})|`},
		{`{.b = 2} && ({.a = 1} >> {} | count() > 1) | sum(.c) > 2`, `and({.b},>>({.a},{})|count() > 1)|sum(.c) > 2`},
	} {
		t.Run(c.query, func(t *testing.T) {
			_, expr := searchStages(t, planSearch(t, c.query))
			if got := describePlan(t, expr); !strings.HasPrefix(got+"|", c.want) {
				t.Errorf("plan = %s, want %s", got, c.want)
			}
		})
	}
}

// Scalar filters read every result span of the expression before them,
// before the spss cap, and set the spanset attributes search returns.
func TestScalarFiltersComeBeforeTheSpanCap(t *testing.T) {
	const query = `{.a = 1} >> {.b = 2} | count() > 2`
	_, expr := searchStages(t, planSearch(t, query))
	if got := describePlan(t, expr); got != `>>({.a},{.b})|count() > 2` {
		t.Errorf("plan = %s", got)
	}
	res := renderPlan(t, planSearch(t, query), testPlannerContext(3))
	if strings.Index(res, "arraySlice(") < strings.Index(res, "count()") {
		t.Errorf("span cap before the scalar filter:\n%s", res)
	}
	for _, col := range []string{"any(result_spans.attrs) as attrs", "as attr_names", "as attr_types", "as attr_values"} {
		if !strings.Contains(res, col) {
			t.Errorf("%q missing:\n%s", col, res)
		}
	}
}

func TestScalarFilterLiterals(t *testing.T) {
	for _, c := range []struct{ query, want string }{
		{`{} | count() >= 3`, `{}|count() >= 3`},
		{`{} | count() = 2.5`, `{}|count() = 2.5`},
		{`{} | avg(duration) > 1.5ms`, `{}|avg(duration) > 1.5e+06`},
		{`{} | avg(duration) > 5`, `{}|avg(duration) > 5`},
		{`{} | sum(.a) < -2s`, `{}|sum(.a) < -2e+09`},
	} {
		t.Run(c.query, func(t *testing.T) {
			_, expr := searchStages(t, planSearch(t, c.query))
			if got := describePlan(t, expr); got != c.want {
				t.Errorf("plan = %s, want %s", got, c.want)
			}
		})
	}
}

// Every scalar filter compares NaN as false and drops spansets without a value.
func TestScalarFiltersDropNaNAndValuelessSpansets(t *testing.T) {
	for _, query := range []string{`{} | count() != 1`, `{} | avg(.a) != 1`, `{} | min(duration) != 1ms`} {
		res := renderPlan(t, planSearch(t, query), testPlannerContext(3))
		if !strings.Contains(res, "n_present > 0 AND NOT isNaN(value) AND value != ") {
			t.Errorf("%s: comparison does not guard NaN and missing values:\n%s", query, res)
		}
	}
}

func TestScalarFilterErrors(t *testing.T) {
	for _, c := range []struct{ query, err string }{
		{`{} | 2 < count()`, "scalar filter lhs of type (%v) not yet supported"},
		{`{} | sum(.a) + 1 > 0`, "scalar filter lhs of type (%v) not yet supported"},
		{`{} | count() / (sum(.n) - sum(.n)) > 0`, "scalar filter lhs of type (%v) not yet supported"},
		{`{} | count() > count()`, "scalar filter rhs of type (%v) not yet supported"},
		{`{} | count() > 1 + 2`, "scalar filter rhs of type (%v) not yet supported"},
		{`{} | avg(status) > 1`, "aggregate field expressions must resolve to a number type: avg(status)"},
		{`{} | sum(name) > 1`, "aggregate field expressions must resolve to a number type: sum(name)"},
		{`{} | max(span:kind) > 1`, "aggregate field expressions must resolve to a number type: max(span:kind)"},
		{`{} | 1 < avg(rootServiceName) + 1`, "aggregate field expressions must resolve to a number type: avg(rootServiceName)"},
		{`({} | min(statusMessage) > 1) && {.a = 1}`, "aggregate field expressions must resolve to a number type: min(statusMessage)"},
		{`{} | count() > 1 | avg(span:childCount) > 1`, "unsupported attribute span:childCount"},
	} {
		t.Run(c.query, func(t *testing.T) {
			q, err := traceql_ir.Parse(c.query)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Plan(q); err == nil || err.Error() != c.err {
				t.Errorf("Plan error = %v, want %s", err, c.err)
			}
			if _, err := PlanMetrics(q, countSpans); err == nil || err.Error() != c.err {
				t.Errorf("PlanMetrics error = %v, want %s", err, c.err)
			}
			if _, err := PlanTagsV2(q); err == nil || err.Error() != c.err {
				t.Errorf("PlanTagsV2 error = %v, want %s", err, c.err)
			}
		})
	}
}

// Metrics count the spans of the traces that pass a scalar filter.
func TestScalarFiltersGateMetrics(t *testing.T) {
	_, expr := metricsResult(t, planMetrics(t, `{.a = 1} >> {.b = 2} | count() > 2`))
	if got := describePlan(t, expr); got != `>>({.a},{.b})|count() > 2` {
		t.Errorf("metrics plan = %s", got)
	}
}

// Every stage reads the date-partitioned tables within the query range.
func TestScalarFilterValuesReadTheQueryRange(t *testing.T) {
	queries := []string{`{.a = 1} | avg(.b) > 1`, `{.a = 1} | avg(b) > 1`, `{.a = 1} | max(duration) > 1ms`}
	for _, query := range queries {
		res := renderPlan(t, planSearch(t, query), testPlannerContext(3))
		if slices.Contains(spanStartBounds(res), ">=86390") {
			t.Errorf("%s reads spans starting at from:\n%s", query, res)
		}
		if !regexp.MustCompile(`_\d+stage1_pivot as \(.*IN \( SELECT trace_id, span_id FROM _\d+stage0\)`).MatchString(res) {
			t.Errorf("%s: values are not read for the spans of the spanset stage:\n%s", query, res)
		}
	}
}
