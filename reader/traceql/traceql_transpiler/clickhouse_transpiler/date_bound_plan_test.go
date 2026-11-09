package clickhouse_transpiler

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
)

var dateBound = regexp.MustCompile(`date\) (>=|<=) \('([0-9-]+)'\)`)

// checkDateBounds requires a lower and an upper bound on each of reads
// date-partitioned reads, a day either side of a range on 2026-11-06 UTC.
func checkDateBounds(t *testing.T, res string, reads int) {
	t.Helper()
	counts := map[string]int{}
	for _, b := range dateBound.FindAllStringSubmatch(res, -1) {
		counts[b[1]]++
		want := map[string]string{">=": "2026-11-05", "<=": "2026-11-07"}[b[1]]
		if b[2] != want {
			t.Errorf("date %s %s, want %s:\n%s", b[1], b[2], want, res)
		}
	}
	if counts[">="] != reads || counts["<="] != reads {
		t.Fatalf("%d lower and %d upper date bounds on %d reads:\n%s", counts[">="], counts["<="], reads, res)
	}
}

// dateBoundContexts are planner contexts over one range, seen from several zones.
func dateBoundContexts() map[string]*shared.PlannerContext {
	res := map[string]*shared.PlannerContext{}
	from := time.Date(2026, 11, 6, 17, 0, 0, 0, time.UTC)
	for name, hours := range map[string]int{"utc": 0, "east": 9, "west": -8, "far east": 14, "far west": -12} {
		zone := time.FixedZone("reader", hours*3600)
		ctx := testPlannerContext(3)
		ctx.From, ctx.To = from.In(zone), from.Add(time.Minute).In(zone)
		ctx.TracesKVDistTable = "tempo_traces_kv"
		res[name] = ctx
	}
	return res
}

func TestSearchDateBoundsDoNotDependOnTheReaderZone(t *testing.T) {
	for _, query := range []string{`{.a = 1}`, `{.a = 1} >> {.b = 2}`, `{.a = .b}`, `{.a = 1} | avg(.b) > 1`, `{.a = 1} | select(.b)`} {
		for zone, ctx := range dateBoundContexts() {
			plan := planSearch(t, query)
			res := renderSearch(t, plan, ctx)
			checkDateBounds(t, res, strings.Count(res, "tempo_traces_attrs_gin"))
			if t.Failed() {
				t.Fatalf("%s, %s", query, zone)
			}
		}
	}
}

func TestAutocompleteDateBoundsDoNotDependOnTheReaderZone(t *testing.T) {
	for _, query := range []string{`{}`, `{.a = 1}`, `{.a = 1} >> {.b = 2}`} {
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
		for _, plan := range []*SearchPlan{tags, values} {
			for zone, ctx := range dateBoundContexts() {
				var res string
				if len(plan.Ids) > 0 {
					res = renderSearch(t, plan, ctx)
				} else {
					res = renderPlan(t, plan.Data, ctx)
				}
				reads := strings.Count(res, "tempo_traces_attrs_gin") + strings.Count(res, "tempo_traces_kv")
				checkDateBounds(t, res, reads)
				if t.Failed() {
					t.Fatalf("%s, %s", query, zone)
				}
			}
		}
	}
}
