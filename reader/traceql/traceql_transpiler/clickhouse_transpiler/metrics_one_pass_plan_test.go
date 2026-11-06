package clickhouse_transpiler

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
)

var tracesReads = regexp.MustCompile(`tempo_traces(_dist)? as `)

// planMetricsQuery plans query, which holds its metrics function.
func planMetricsQuery(t *testing.T, query string) *MetricsPlan {
	t.Helper()
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

// onePassSelectors relate no spans across a trace.
var onePassSelectors = []string{
	`{}`,
	`({})`,
	`{.a = 1}`,
	`({.a = 1})`,
	`{.a = 1} | select(.x)`,
	`{} | select(.x)`,
	`{.a = 1} | {.b = 2}`,
	`{duration > 1s}`,
	`{duration * 2 > 1s}`,
	`{nestedSetParent < 0}`,
	`{nestedSetLeft > 2}`,
	`{nestedSetLeft > 2} | select(.x)`,
	`{nestedSetLeft > 2 && .a = 1}`,
	`{nestedSetParent < 0 && resource.service.name = "a"}`,
	`{nestedSetParent < 0 || .a = 1}`,
	`{nestedSetParent < 0 && nestedSetLeft > 2 && .a = 1}`,
}

// A query that relates no spans across a trace reads the traces table once and
// runs no ids statement, also with a sample, a nested-set field or exemplars.
func TestMetricsWithoutWholeTraceReadTracesOnce(t *testing.T) {
	for _, selector := range onePassSelectors {
		for _, fn := range []string{`rate()`, `rate() with(sample=0.25)`, `avg_over_time(nestedSetRight) by (nestedSetLeft)`} {
			query := selector + " | " + fn
			for _, cluster := range []bool{false, true} {
				t.Run(query, func(t *testing.T) {
					plan := planMetricsQuery(t, query)
					if len(plan.Ids) != 0 {
						t.Errorf("%d ids statements, want none", len(plan.Ids))
					}
					ctx := testPlannerContext(3)
					ctx.IsCluster = cluster
					over, err := plan.Over(countSpans)
					if err != nil {
						t.Fatal(err)
					}
					for _, stmt := range []string{renderPlan(t, plan.Metrics, ctx), renderPlan(t, over, ctx)} {
						if got := len(tracesReads.FindAllString(stmt, -1)); got != 1 {
							t.Errorf("%d traces reads, want 1:\n%s", got, stmt)
						}
						if !strings.Contains(stmt, "GROUP BY traces.trace_id, traces.span_id") {
							t.Errorf("read is not grouped by span:\n%s", stmt)
						}
						if !strings.Contains(stmt, "(traces.timestamp_ns) >") {
							t.Errorf("read has no span start bound:\n%s", stmt)
						}
						if cluster {
							assertOnEachShard(t, stmt, ctx)
						}
					}
				})
			}
		}
	}
}

// A query that relates spans across a trace, also with by() or a scalar filter
// over one selector, lists its span ids in an ids statement.
func TestMetricsOverWholeTracesKeepTheIdsStatement(t *testing.T) {
	for _, query := range []string{
		`{.a = 1} >> {.b = 2}`,
		`{.a = 1} ~ {.b = 2}`,
		`{.a = 1} && {.b = 2}`,
		`{.a = 1} || {.b = 2}`,
		`{.a = 1} | count() > 2`,
		`{.a = 1} | by(.b)`,
	} {
		t.Run(query, func(t *testing.T) {
			plan := planMetrics(t, query)
			if len(plan.Ids) != 1 {
				t.Fatalf("%d ids statements, want 1", len(plan.Ids))
			}
			res := renderPlan(t, plan.Metrics, testPlannerContext(3))
			if !strings.Contains(res, SearchResultTable) {
				t.Errorf("metrics statement doesn't read %s:\n%s", SearchResultTable, res)
			}
		})
	}
}

// A metric's by(name) stays on one read: it's Metrics.By, not the pipeline stage.
func TestMetricsByLabelsReadTracesOnce(t *testing.T) {
	plan := planMetricsQuery(t, `{.a = 1} | rate() by (name)`)
	if len(plan.Ids) != 0 {
		t.Errorf("%d ids statements, want none", len(plan.Ids))
	}
	res := renderPlan(t, plan.Metrics, testPlannerContext(3))
	if got := len(tracesReads.FindAllString(res, -1)); got != 1 {
		t.Errorf("%d traces reads, want 1:\n%s", got, res)
	}
}

// A sample lands on the operand read, hashed by span.
func TestOnePassMetricsSampleTheirSelector(t *testing.T) {
	for _, query := range []string{`{}`, `{.a = 1}`, `{nestedSetLeft > 2}`, `{nestedSetParent < 0 && .a = 1}`} {
		t.Run(query, func(t *testing.T) {
			res := renderPlan(t, planMetricsQuery(t, query+` | rate() with(sample=0.25)`).Metrics, testPlannerContext(3))
			if got := strings.Count(res, "cityHash64(span_id) < 4611686018427387904"); got != 1 {
				t.Errorf("%d samples, want 1:\n%s", got, res)
			}
		})
	}
}

// A nested-set field hangs off the read, keyed by the read's own trace ids.
func TestOnePassMetricsJoinNestedSetOverTheirOwnIds(t *testing.T) {
	for _, query := range []string{`{nestedSetLeft > 2} | rate()`, `{.a = 1} | avg_over_time(nestedSetRight)`, `{} | rate() by (nestedSetLeft)`} {
		t.Run(query, func(t *testing.T) {
			res := renderPlan(t, planMetricsQuery(t, query).Metrics, testPlannerContext(3))
			if !regexp.MustCompile(`ns\.trace_id IN \( SELECT trace_id FROM \w*metrics_traces\)`).MatchString(res) {
				t.Errorf("nested-set values aren't keyed by the read's ids:\n%s", res)
			}
		})
	}
}

// A selector the read can't filter itself gives the read its spans.
func TestOnePassMetricsAreTheResultSpans(t *testing.T) {
	for _, query := range []string{`{.a = 1}`, `{.a = 1} | {.b = 2}`, `{duration > 1s}`, `{nestedSetLeft > 2 && .a = 1}`} {
		t.Run(query, func(t *testing.T) {
			res := renderPlan(t, planMetrics(t, query).Metrics, testPlannerContext(3))
			if !regexp.MustCompile(`\(traces\.trace_id, traces\.span_id\) IN \( SELECT trace_id, span_id FROM \w+result_spans\)`).MatchString(res) {
				t.Errorf("metrics spans are not the result spans by (trace_id, span_id):\n%s", res)
			}
		})
	}
}

// A root filter beside an attribute reads the root flag from the one read, and
// keeps the attribute's values and the flag in one filter.
func TestOnePassMetricsFoldARootFilterOntoTheRead(t *testing.T) {
	res := renderPlan(t, planMetricsQuery(t, `{nestedSetParent < 0 && .a = "1"} | rate()`).Metrics, testPlannerContext(3))
	for _, want := range []string{"max(traces.parent_id = '') as _root", "(traces.trace_id, traces.span_id) IN", "inner JOIN", "_root, 0"} {
		if !strings.Contains(res, want) {
			t.Errorf("no %q in:\n%s", want, res)
		}
	}
}

// compare()'s selection reads traces in the statement, and so does its baseline
// unless the query relates spans across a trace.
func TestCompareSidesReadTracesOnceEach(t *testing.T) {
	for _, tc := range []struct {
		query string
		ids   int
		reads int
	}{
		{`{} | compare({})`, 0, 1},
		{`{} | compare({.c = 3})`, 0, 2},
		{`{.a = 1} | compare({.c = 3})`, 0, 2},
		{`{.a = 1} | select(.x) | compare({nestedSetParent < 0})`, 0, 2},
		{`{nestedSetParent < 0 && .a = 1} | compare({nestedSetParent < 0 && .c = 3})`, 0, 2},
		{`({.a = 1} >> {.b = 2}) | compare({})`, 1, 1},
		{`({.a = 1} >> {.b = 2}) | compare({.c = 3})`, 1, 2},
	} {
		for _, cluster := range []bool{false, true} {
			t.Run(tc.query, func(t *testing.T) {
				q, err := traceql_ir.Parse(tc.query)
				if err != nil {
					t.Fatal(err)
				}
				plan, err := PlanCompare(q, q.Metrics.Compare.Filter, time.Unix(86000, 0), time.Unix(86500, 0), compareAttrRows)
				if err != nil {
					t.Fatal(err)
				}
				if len(plan.Ids) != tc.ids {
					t.Errorf("%d ids statements, want %d", len(plan.Ids), tc.ids)
				}
				ctx := testPlannerContext(3)
				ctx.IsCluster = cluster
				res := renderPlan(t, plan.Metrics, ctx)
				if got := len(tracesReads.FindAllString(res, -1)); got != tc.reads {
					t.Errorf("%d traces reads, want %d:\n%s", got, tc.reads, res)
				}
				if cluster {
					assertOnEachShard(t, res, ctx)
				}
			})
		}
	}
}

// assertOnEachShard wants stmt to read the traces over local tables on each shard.
func assertOnEachShard(t *testing.T, stmt string, ctx *shared.PlannerContext) {
	t.Helper()
	for _, want := range []string{"cluster('", "view(", "prefer_localhost_replica"} {
		if !strings.Contains(stmt, want) {
			t.Errorf("no %q in:\n%s", want, stmt)
		}
	}
	for _, dist := range []string{"tempo_traces_dist", "tempo_traces_nested_set_dist", ctx.TracesAttrsDistTable} {
		if strings.Contains(stmt, dist) {
			t.Errorf("reads %s on a shard:\n%s", dist, stmt)
		}
	}
}

// skip_unavailable_shards sits on the read of the shards, not the outer statement.
func TestOnePassMetricsSkipUnavailableShardsOnTheRead(t *testing.T) {
	plan := planMetricsQuery(t, `{.a = 1} | rate()`)
	over, err := plan.Over(countSpans)
	if err != nil {
		t.Fatal(err)
	}
	for _, skip := range []bool{false, true} {
		ctx := testPlannerContext(3)
		ctx.IsCluster = true
		ctx.SkipUnavailableShards = skip
		for _, stmt := range []string{renderPlan(t, plan.Metrics, ctx), renderPlan(t, over, ctx)} {
			setting := regexp.MustCompile(`metrics_spans as \(.* SETTINGS [^)]*skip_unavailable_shards=1`)
			if setting.MatchString(stmt) != skip || strings.Count(stmt, "skip_unavailable_shards") > 1 {
				t.Errorf("want skip_unavailable_shards once on the read = %v:\n%s", skip, stmt)
			}
			if strings.HasSuffix(stmt, "skip_unavailable_shards=1") {
				t.Errorf("skip_unavailable_shards on the outer statement:\n%s", stmt)
			}
		}
	}
}
