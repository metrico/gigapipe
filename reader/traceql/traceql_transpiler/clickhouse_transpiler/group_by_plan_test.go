package clickhouse_transpiler

import (
	"regexp"
	"strings"
	"testing"

	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
)

func TestGroupingStagesPlanOneCTEEach(t *testing.T) {
	for _, c := range []struct {
		query, plan string
	}{
		{`{.a = 1} | by(.b)`, `{.a}|by(.b)`},
		{`{.a = 1} | by(.b) | by(.c + 1) | count() > 1 | coalesce() | {.d = 2}`,
			`{.a}|by(.b)|by(.c + 1)|count() > 1|coalesce()|{.d}`},
		// A leading by() reads every span in range first.
		{`by(.b)`, `{}|by(.b)`},
		{`{.a = 1} >> {.b = 2} | by(.c)`, `>>({.a},{.b})|by(.c)`},
		{`({.a = 1} | by(.b) | coalesce()) && {.c = 2}`, `and({.a}|by(.b)|coalesce(),{.c})`},
	} {
		t.Run(c.query, func(t *testing.T) {
			_, expr := searchStages(t, planSearch(t, c.query))
			if got := describePlan(t, expr); got != c.plan {
				t.Errorf("search plan = %s, want %s", got, c.plan)
			}
			_, expr = metricsResult(t, planMetrics(t, c.query))
			if got := describePlan(t, expr); got != c.plan {
				t.Errorf("metrics plan = %s, want %s", got, c.plan)
			}
			res := renderSearch(t, planSearch(t, c.query), testPlannerContext(3))
			if n := len(stageCTEs(res)); n != strings.Count(c.plan, "|")+1 && !strings.HasPrefix(c.plan, "and(") {
				t.Errorf("%d stage CTEs for %s:\n%s", n, c.plan, res)
			}
		})
	}
}

// by() appends its key to every span's group key and, named by the stage, to
// the spanset attributes; coalesce() clears both.
func TestGroupByAppendsTheKeyAndCoalesceClearsIt(t *testing.T) {
	res := renderSearch(t, planSearch(t, `{.a = 1} | by(.b) | coalesce()`), testPlannerContext(3))
	for _, want := range []string{
		`arrayPushBack(grp, key) as grp`,
		`arrayPushBack(attrs, tuple('by(.b)', tupleElement(key, 4), tupleElement(key, 2), tupleElement(key, 3))) as attrs`,
		`CAST([], 'Array(Tuple(String, String, Float64, String))') as attrs, CAST([], 'Array(Tuple(UInt8, Float64, String, String))') as grp FROM _`,
	} {
		if !strings.Contains(res, want) {
			t.Errorf("%q missing:\n%s", want, res)
		}
	}
	if !regexp.MustCompile(`_\d+stage2 as \( SELECT trace_id as trace_id, span_id as span_id, timestamp_ns as timestamp_ns, CAST\(\[\]`).MatchString(res) {
		t.Errorf("coalesce() keeps attributes or group keys:\n%s", res)
	}
}

// Scalar filters aggregate per spanset: per trace and group key.
func TestScalarFiltersAggregatePerGroup(t *testing.T) {
	for _, query := range []string{`{.a = 1} | by(.b) | count() > 1`, `{.a = 1} | by(.b) | avg(.c) > 1`} {
		res := renderSearch(t, planSearch(t, query), testPlannerContext(3))
		if !regexp.MustCompile(`OVER \(PARTITION BY (_\d+stage1\.)?trace_id, (_\d+stage1\.)?grp\)`).MatchString(res) {
			t.Errorf("%s: aggregate is not per group:\n%s", query, res)
		}
	}
}

// The key reads the pivot of the spans of the stage before; spans lacking
// every key read are in the pivot when their key may be other than nil.
func TestGroupByKeyReadsThePreviousStageSpans(t *testing.T) {
	for _, c := range []struct {
		query string
		keys  string
	}{
		{`{.a = 1} | by(.b)`, `traces_idx.key IN ('b')`},
		{`{.a = 1} | by(.b + .c)`, `traces_idx.key IN ('b', 'c')`},
		{`{.a = 1} | by(.b = nil)`, `traces_idx.key IN ('b', 'name')`},
		{`{.a = 1} | by(.b > 1)`, `traces_idx.key IN ('b', 'name')`},
		{`{.a = 1} | by(name)`, `traces_idx.key IN ('name')`},
		{`{.a = 1} | by(duration)`, `FROM tempo_traces as traces`},
	} {
		t.Run(c.query, func(t *testing.T) {
			res := renderSearch(t, planSearch(t, c.query), testPlannerContext(3))
			pivot := regexp.MustCompile(`_\d+stage1_pivot as \(.*?GROUP BY`).FindString(res)
			if !strings.Contains(pivot, c.keys) {
				t.Errorf("pivot does not read %s:\n%s", c.keys, pivot)
			}
			if !regexp.MustCompile(`IN \( SELECT trace_id, span_id FROM _\d+stage0\)`).MatchString(pivot) {
				t.Errorf("pivot is not restricted to the previous stage:\n%s", pivot)
			}
		})
	}
}

// Search lists one spanset per trace and group key, a trace's spansets by
// key, and limits traces.
func TestSearchListsSpansetsPerGroup(t *testing.T) {
	res := renderSearch(t, planSearch(t, `{.a = 1} | by(.b)`), testPlannerContext(3))
	for _, want := range []string{
		`FROM result_spans GROUP BY trace_id, grp`,
		`arraySort(x -> x.1, groupArray((grp, span_ids, matched, attrs))) as spansets FROM result_spansets GROUP BY trace_id`,
		`arrayMap(s -> s.3, spansets) as matched`,
	} {
		if !strings.Contains(res, want) {
			t.Errorf("%q missing:\n%s", want, res)
		}
	}
}

// Metrics count the spans of the groups that pass: group keys are no
// series dimension.
func TestGroupsFlattenForMetrics(t *testing.T) {
	res := renderPlan(t, planMetrics(t, `{.a = 1} | by(.b) | count() > 1`).Metrics, testPlannerContext(3))
	spans := regexp.MustCompile(`metrics_spans as \(.*GROUP BY traces.trace_id, traces.span_id\)`).FindString(res)
	if spans == "" || strings.Contains(res[strings.Index(res, "metrics_spans as ("):], "grp") {
		t.Errorf("metrics read group keys:\n%s", res)
	}
}

func TestGroupingPlanErrors(t *testing.T) {
	const grouped = "spanset operators are not supported for multiple spansets per trace. consider using coalesce()"
	for _, c := range []struct{ query, err string }{
		{`{.a = 1} | by(.b) | {.c = 2} >> {.d = 3}`, grouped},
		{`{.a = 1} | by(.b) | {.c = 2} && {.d = 3}`, grouped},
		{`({.a = 1} | by(.b)) || {.c = 2}`, grouped},
		{`{.c = 2} ~ ({.a = 1} | by(.b) | count() > 1)`, grouped},
		{`{.a = 1} | by(1)`, "grouping field expressions must reference the span: by(1)"},
		{`{.a = 1} | by(status = "x")`, `binary operations must operate on the same type: status = "x"`},
		{`{.a = 1} | by(span:id)`, "unsupported attribute span:id"},
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
