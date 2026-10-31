package clickhouse_transpiler

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
)

var sampleCond = regexp.MustCompile(`cityHash64\((trace_id|span_id)\) < (\d+)`)

// sampleConds counts the sample conditions of a rendered plan by hashed
// column, each with the threshold of 0.25.
func sampleConds(t *testing.T, res string) map[string]int {
	t.Helper()
	out := map[string]int{}
	for _, m := range sampleCond.FindAllStringSubmatch(res, -1) {
		if m[2] != "4611686018427387904" {
			t.Errorf("sample threshold %s, want 0.25·2^64", m[2])
		}
		out[m[1]]++
	}
	return out
}

func planSampledMetrics(t *testing.T, query string) string {
	t.Helper()
	q, err := traceql_ir.Parse(query + " | rate() with(sample=0.25)")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := PlanMetrics(q, countSpans)
	if err != nil {
		t.Fatal(err)
	}
	if res := renderPlan(t, plan.Metrics, testPlannerContext(3)); sampleCond.MatchString(res) {
		t.Errorf("metrics statement samples:\n%s", res)
	}
	return renderPlan(t, plan.Ids[0].Statement, testPlannerContext(3))
}

// sample hashes the trace id in every operand read when the query relates
// spans across a trace, and the span id otherwise.
func TestMetricsSampleEveryOperandRead(t *testing.T) {
	for _, c := range []struct {
		query string
		// reads is the number of operand reads.
		reads  int
		column string
	}{
		{`{.a = 1}`, 1, "span_id"},
		{`{.a = .b}`, 1, "span_id"},
		{`{}`, 1, "span_id"},
		{`{.a = 1} | {.b = 2} | select(.x)`, 2, "span_id"},
		{`{.a = 1} && {.b = 2}`, 2, "trace_id"},
		{`{.a = 1} || {.b = .c}`, 2, "trace_id"},
		{`{.a = 1} > {.b = 2}`, 2, "trace_id"},
		{`{.a = 1} >> {}`, 2, "trace_id"},
		{`{.a = 1} | count() > 1`, 1, "trace_id"},
		{`count() > 1`, 1, "trace_id"},
		{`{.a = 1} | by(.b)`, 1, "trace_id"},
		{`{.a = 1} | ({.b = 2} | count() > 1)`, 2, "trace_id"},
	} {
		t.Run(c.query, func(t *testing.T) {
			res := planSampledMetrics(t, c.query)
			got := sampleConds(t, res)
			if len(got) != 1 || got[c.column] != c.reads {
				t.Errorf("sample conditions %v, want %d on %s:\n%s", got, c.reads, c.column, res)
			}
		})
	}
}

var nodesCTE = regexp.MustCompile(`_nodes as \((.*?)\),_\d+`)

// The trees of sampled traces come unsampled, so ancestry sees the whole tree.
func TestSampledAncestryReadsTheWholeTree(t *testing.T) {
	for _, query := range []string{`{.a = 1} >> {.b = 2}`, `{.a = 1} > {.b = 2}`} {
		res := planSampledMetrics(t, query)
		nodes := nodesCTE.FindStringSubmatch(res)
		if nodes == nil {
			t.Fatalf("%s: no nodes CTE:\n%s", query, res)
		}
		if strings.Contains(nodes[1], "cityHash64") {
			t.Errorf("%s: nodes read sampled:\n%s", query, nodes[1])
		}
	}
}

// compare() samples its baseline and its selection alike.
func TestCompareSamplesBaselineAndSelection(t *testing.T) {
	q, err := traceql_ir.Parse(`{.a = 1} | compare({.b = 2}) with(sample=0.25)`)
	if err != nil {
		t.Fatal(err)
	}
	from := time.Unix(86390, 0)
	plan, err := PlanCompare(q, q.Metrics.Compare.Filter, from, from.Add(time.Minute), compareAttrRows)
	if err != nil {
		t.Fatal(err)
	}
	for _, ids := range plan.Ids {
		if got := sampleConds(t, renderPlan(t, ids.Statement, testPlannerContext(3))); got["span_id"] != 1 || len(got) != 1 {
			t.Errorf("%s: sample conditions %v, want 1 on span_id", ids.Table, got)
		}
	}
	if res := renderPlan(t, plan.Metrics, testPlannerContext(3)); sampleCond.MatchString(res) {
		t.Errorf("compare statement samples:\n%s", res)
	}
}

// Without a sample fraction in (0, 1), nothing is sampled; search and
// autocomplete never sample.
func TestSampleOnlyAppliesToMetricsFractions(t *testing.T) {
	for _, hints := range []string{``, ` with(sample=true)`, ` with(sample=1)`, ` with(sample=0)`, ` with(most_recent=true, foo=0.5)`} {
		q, err := traceql_ir.Parse(`{.a = 1} >> {} | rate()` + hints)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := PlanMetrics(q, countSpans)
		if err != nil {
			t.Fatal(err)
		}
		if res := renderMetrics(t, plan, testPlannerContext(3)); sampleCond.MatchString(res) {
			t.Errorf("%s samples:\n%s", hints, res)
		}
	}
	query := `{.a = 1} >> {} with(sample=0.25, exemplars=2)`
	plans := map[string]shared.SQLRequestPlanner{"search": planSearch(t, query).Ids[0].Statement}
	for kind, plan := range autocompletePlans(t, query) {
		plans[kind+" search"], plans[kind+" data"] = plan.Ids[0].Statement, plan.Data
	}
	for kind, plan := range plans {
		if res := renderPlan(t, plan, testPlannerContext(3)); sampleCond.MatchString(res) {
			t.Errorf("%s samples:\n%s", kind, res)
		}
	}
}

func TestSampleThreshold(t *testing.T) {
	for f, want := range map[float64]uint64{
		0.5:             1 << 63,
		0.25:            1 << 62,
		1 - 1.0/(1<<53): 1<<64 - 1<<11,
		1.0 / (1 << 63): 2,
		1e-30:           0,
	} {
		if got := sampleThreshold(f); got != want {
			t.Errorf("sampleThreshold(%g) = %d, want %d", f, got, want)
		}
	}
}
