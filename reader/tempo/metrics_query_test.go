package tempo

import (
	"regexp"
	"strings"
	"testing"

	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

func testSpans() *sql.With {
	return sql.NewWith(sql.NewSelect().
		Select(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id")).
		From(sql.NewRawObject("result")), "metrics_spans")
}

func render(t *testing.T, q sql.ISelect) string {
	t.Helper()
	res, err := q.String(sql.DefaultCtx())
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// Every metrics and exemplar query reads its spans from the spans CTE only.
func TestMetricsQueriesReadOnlyTheSpans(t *testing.T) {
	traces := regexp.MustCompile(`tempo_traces\b`)
	req := func(fn, attr string, by ...string) *MetricsRequest {
		return &MetricsRequest{Spans: testSpans(), Fn: fn, Attr: attr, By: by,
			FromNS: 10e9, ToNS: 20e9, StepNS: 1e9, AttrsTable: "tempo_traces_attrs_gin"}
	}
	var queries []sql.ISelect
	for _, r := range []*MetricsRequest{
		req("rate", ""), req("count_over_time", "", "name", "span.http.method"),
		req("avg_over_time", "duration"), req("quantile_over_time", "duration", "service.name"),
	} {
		q, _, err := BuildMetricsRangeQuery(r)
		if err != nil {
			t.Fatal(err)
		}
		queries = append(queries, q)
		if q, _, err = BuildMetricsInstantQuery(r); err != nil {
			t.Fatal(err)
		}
		queries = append(queries, q)
		if q, _, err = BuildGenericExemplarsQuery(r); err != nil {
			t.Fatal(err)
		}
		queries = append(queries, q)
	}
	for _, build := range []func(*MetricsRequest) (sql.ISelect, error){BuildHistogramRangeQuery, BuildHistogramExemplarsQuery} {
		q, err := build(req("histogram_over_time", "duration"))
		if err != nil {
			t.Fatal(err)
		}
		queries = append(queries, q)
	}
	for _, q := range queries {
		res := render(t, q)
		if strings.Contains(res, "any(main.") && !strings.Contains(res, exemplarSpan) {
			t.Errorf("exemplar columns are not read from one span:\n%s", res)
		}
		if !strings.Contains(res, "FROM metrics_spans as main") || !strings.HasPrefix(res, "WITH metrics_spans as (") {
			t.Errorf("does not read the spans:\n%s", res)
		}
		if traces.MatchString(res) {
			t.Errorf("reads the traces table:\n%s", res)
		}
	}
}

func TestByLabelsComeInColumnOrder(t *testing.T) {
	r := &MetricsRequest{Spans: testSpans(), Fn: "rate", By: []string{"span.a", "name", ".b", "service.name"},
		FromNS: 10e9, ToNS: 20e9, StepNS: 1e9, AttrsTable: "attrs"}
	for _, build := range []func(*MetricsRequest) (sql.ISelect, []string, error){
		BuildMetricsRangeQuery, BuildMetricsInstantQuery, BuildGenericExemplarsQuery} {
		q, labels, err := build(r)
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(labels, ","); got != "name,service.name,span.a,.b" {
			t.Errorf("labels %s", got)
		}
		res := render(t, q)
		cols := regexp.MustCompile(`as (by_\d+|by_custom_\d+)`).FindAllStringSubmatch(res, -1)
		var got []string
		for _, c := range cols {
			got = append(got, c[1])
		}
		if strings.Join(got, ",") != "by_1,by_3,by_custom_0,by_custom_1" {
			t.Errorf("by columns %v:\n%s", got, res)
		}
	}
}

// A by attribute is pivoted on its storage key, without its scope.
func TestByAttributeReadsItsStorageKey(t *testing.T) {
	for label, key := range map[string]string{
		".foo": "foo", "span.foo": "foo", "resource.foo": "foo", ".5xx": "5xx", ".span.foo": "span.foo",
	} {
		t.Run(label, func(t *testing.T) {
			r := &MetricsRequest{Spans: testSpans(), Fn: "rate", By: []string{label},
				FromNS: 10e9, ToNS: 20e9, StepNS: 1e9, AttrsTable: "attrs"}
			q, labels, err := BuildMetricsRangeQuery(r)
			if err != nil {
				t.Fatal(err)
			}
			if labels[0] != label {
				t.Errorf("label %s", labels[0])
			}
			res := render(t, q)
			if want := "anyIf(val, key = '" + key + "')"; !strings.Contains(res, want) {
				t.Errorf("want %s in:\n%s", want, res)
			}
			if want := "key IN ('" + key + "')"; !strings.Contains(res, want) {
				t.Errorf("want %s in:\n%s", want, res)
			}
		})
	}
}

// Series labels hold the plain name; the pivot reads its key.
func TestByLabelsHoldThePlainName(t *testing.T) {
	r := &MetricsRequest{Spans: testSpans(), Fn: "rate", By: []string{"span.http status", ".5xx", `.a"b`},
		FromNS: 10e9, ToNS: 20e9, StepNS: 1e9, AttrsTable: "attrs"}
	for _, build := range []func(*MetricsRequest) (sql.ISelect, []string, error){
		BuildMetricsRangeQuery, BuildMetricsInstantQuery, BuildGenericExemplarsQuery} {
		q, labels, err := build(r)
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(labels, ","); got != `span.http status,.5xx,.a"b` {
			t.Errorf("labels %s", got)
		}
		if res := render(t, q); !strings.Contains(res, `key IN ('http status','5xx','a"b')`) {
			t.Errorf("pivot keys in:\n%s", res)
		}
	}
}

// A by key holding a backslash and a quote stays one string literal.
func TestByKeyRendersAsOneStringLiteral(t *testing.T) {
	r := &MetricsRequest{Spans: testSpans(), Fn: "rate", By: []string{`span.a\' OR 1=1 --`},
		FromNS: 10e9, ToNS: 20e9, StepNS: 1e9, AttrsTable: "attrs"}
	q, _, err := BuildMetricsRangeQuery(r)
	if err != nil {
		t.Fatal(err)
	}
	res := render(t, q)
	if want := `anyIf(val, key = 'a\\\' OR 1=1 --')`; !strings.Contains(res, want) {
		t.Errorf("want %s in:\n%s", want, res)
	}
}

func TestCompareReadsBaselineAndSelection(t *testing.T) {
	baseline := sql.NewWith(sql.NewSelect().Select(sql.NewRawObject("trace_id")).From(sql.NewRawObject("b")), "baseline_spans")
	selection := sql.NewWith(sql.NewSelect().Select(sql.NewRawObject("trace_id")).From(sql.NewRawObject("s")), "selection_spans")
	req := &CompareRequest{Baseline: baseline, Selection: selection, FromNS: 100e9, ToNS: 200e9,
		BaselineFromNS: 50e9, BaselineToNS: 150e9, AttrsTable: "attrs", TopN: 3}
	res := render(t, mustCompare(t, req))
	for _, want := range []string{
		"WITH baseline_spans as (", "selection_spans as (",
		"all_spans as ( SELECT trace_id, span_id FROM baseline_spans)",
		"selection as ( SELECT trace_id, span_id FROM selection_spans)",
		"(date) >= ('1969-12-31')", "(date) <= ('1970-01-02')", "(rn) <= (3)",
	} {
		if !strings.Contains(res, want) {
			t.Errorf("missing %q:\n%s", want, res)
		}
	}
	req.Selection = nil
	res = render(t, mustCompare(t, req))
	if !strings.Contains(res, "selection as ( SELECT trace_id, span_id FROM all_spans)") {
		t.Errorf("compare without a selection does not select every baseline span:\n%s", res)
	}
}

func mustCompare(t *testing.T, req *CompareRequest) sql.ISelect {
	t.Helper()
	q, err := BuildCompareQuery(req)
	if err != nil {
		t.Fatal(err)
	}
	return q
}
