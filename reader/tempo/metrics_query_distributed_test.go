package tempo

import (
	"regexp"
	"strings"
	"testing"

	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// On a cluster the spans table and the attributes index are both Distributed
// tables. A plain JOIN of one with a subquery over the other is a
// double-distributed JOIN, which ClickHouse rejects with error 288 under
// distributed_product_mode='deny'. These tests pin that every join from the
// spans table to an attributes CTE is GLOBAL on a cluster, and that a single
// node gets no GLOBAL at all.

const (
	fromNS = int64(1700000000000000000)
	toNS   = int64(1700003600000000000)
	stepNS = int64(60e9)
)

var attrCTEs = []string{"filtered", "by_attrs", "outer_filtered", "inner_filtered"}

var joinRe = regexp.MustCompile(`((?:[A-Z]+ )+)JOIN (\w+)`)

func metricsTables(distributed bool) (string, string) {
	if distributed {
		return "`d`.tempo_traces_dist", "`d`.tempo_traces_attrs_gin_dist"
	}
	return "`d`.tempo_traces", "`d`.tempo_traces_attrs_gin"
}

func render(t *testing.T, q sql.ISelect, err error) string {
	t.Helper()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	s, err := q.String(&sql.Ctx{})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	return s
}

// metricsBuilders builds every TraceQL metrics query that joins the attributes
// index, filtered by span.env and, where the builder supports it, grouped by it.
var metricsBuilders = map[string]func(t *testing.T, distributed bool) string{
	"range": func(t *testing.T, d bool) string {
		traces, attrs := metricsTables(d)
		q, _, err := BuildMetricsRangeQuery("count_over_time", "", []string{"span.env"},
			[]string{"span.env"}, []string{"="}, []string{"a"},
			fromNS, toNS, stepNS, traces, attrs, d, nil)
		return render(t, q, err)
	},
	"instant": func(t *testing.T, d bool) string {
		traces, attrs := metricsTables(d)
		q, _, err := BuildMetricsInstantQuery("count_over_time", "", []string{"span.env"},
			[]string{"span.env"}, []string{"="}, []string{"a"},
			fromNS, toNS, traces, attrs, d, nil)
		return render(t, q, err)
	},
	"histogram": func(t *testing.T, d bool) string {
		traces, attrs := metricsTables(d)
		q, err := BuildHistogramRangeQuery(&HistogramRequest{FromNS: fromNS, ToNS: toNS, StepNS: stepNS,
			TracesTable: traces, AttrsTable: attrs, Distributed: d, Attr: "duration",
			FilterKeys: []string{"span.env"}, FilterOps: []string{"="}, FilterVals: []string{"a"}})
		return render(t, q, err)
	},
	"histogram exemplars": func(t *testing.T, d bool) string {
		traces, attrs := metricsTables(d)
		q, err := BuildHistogramExemplarsQuery(&HistogramRequest{FromNS: fromNS, ToNS: toNS, StepNS: stepNS,
			TracesTable: traces, AttrsTable: attrs, Distributed: d, Attr: "duration",
			FilterKeys: []string{"span.env"}, FilterOps: []string{"="}, FilterVals: []string{"a"}})
		return render(t, q, err)
	},
	"exemplars": func(t *testing.T, d bool) string {
		traces, attrs := metricsTables(d)
		q, _, err := BuildGenericExemplarsQuery(&GenericExemplarsRequest{FromNS: fromNS, ToNS: toNS, StepNS: stepNS,
			TracesTable: traces, AttrsTable: attrs, Distributed: d, ByLabels: []string{"span.env"},
			FilterKeys: []string{"span.env"}, FilterOps: []string{"="}, FilterVals: []string{"a"}})
		return render(t, q, err)
	},
	"compare": func(t *testing.T, d bool) string {
		traces, attrs := metricsTables(d)
		q, err := BuildCompareQuery(&CompareRequest{FromNS: fromNS, ToNS: toNS,
			TracesTable: traces, AttrsTable: attrs, Distributed: d,
			OuterKeys: []string{"span.testId"}, OuterOps: []string{"="}, OuterVals: []string{"x"},
			InnerKeys: []string{"span.env"}, InnerOps: []string{"="}, InnerVals: []string{"b"}})
		return render(t, q, err)
	},
}

func TestMetricsAttrJoinsAreGlobalOnCluster(t *testing.T) {
	for name, build := range metricsBuilders {
		t.Run(name, func(t *testing.T) {
			query := build(t, true)
			found := 0
			for _, m := range joinRe.FindAllStringSubmatch(query, -1) {
				for _, cte := range attrCTEs {
					if m[2] != cte {
						continue
					}
					found++
					if !strings.HasPrefix(m[1], "GLOBAL ") {
						t.Errorf("join to %s is %q, want a GLOBAL join", cte, m[1]+"JOIN")
					}
				}
			}
			if found == 0 {
				t.Fatalf("no join to an attributes CTE in:\n%s", query)
			}
		})
	}
}

func TestMetricsJoinsAreLocalOnSingleNode(t *testing.T) {
	for name, build := range metricsBuilders {
		t.Run(name, func(t *testing.T) {
			if query := build(t, false); strings.Contains(query, "GLOBAL") {
				t.Errorf("single-node query has a GLOBAL join:\n%s", query)
			}
		})
	}
}
