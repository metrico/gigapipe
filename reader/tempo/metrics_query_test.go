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

// Volume functions scale by Scale; values that don't depend on volume don't.
func TestMetricsScale(t *testing.T) {
	p := 0.9
	for _, tc := range []struct {
		fn, attr string
		// plain renders at Scale 1, range and instant at Scale 4.
		plain, instantPlain, scaledRange, scaledInstant string
	}{
		{fn: "rate",
			plain:         "toFloat64(count(*)) / 1.000000",
			instantPlain:  "toFloat64(count(*)) / 10.000000",
			scaledRange:   "toFloat64(count(*)) / 0.25",
			scaledInstant: "toFloat64(count(*)) / 2.5"},
		{fn: "count_over_time",
			plain:       "count(*)",
			scaledRange: "(count(*)) * 4"},
		{fn: "sum_over_time", attr: "duration",
			plain:       "sum(main.duration_ns) / 1e9",
			scaledRange: "(sum(main.duration_ns) / 1e9) * 4"},
		{fn: "avg_over_time", attr: "duration", plain: "avg(main.duration_ns) / 1e9"},
		{fn: "min_over_time", attr: "duration", plain: "min(main.duration_ns) / 1e9"},
		{fn: "max_over_time", attr: "duration", plain: "max(main.duration_ns) / 1e9"},
		{fn: "quantile_over_time", attr: "duration", plain: "quantile(0.9)(main.duration_ns) / 1e9"},
	} {
		t.Run(tc.fn, func(t *testing.T) {
			if tc.instantPlain == "" {
				tc.instantPlain = tc.plain
			}
			if tc.scaledRange == "" {
				tc.scaledRange = tc.plain
			}
			if tc.scaledInstant == "" {
				tc.scaledInstant = tc.scaledRange
			}
			req := func(scale float64) *MetricsRequest {
				return &MetricsRequest{Spans: testSpans(), Fn: tc.fn, Attr: tc.attr, Percentile: &p,
					FromNS: 10e9, ToNS: 20e9, StepNS: 1e9, AttrsTable: "attrs", Scale: scale}
			}
			rangeSQL := func(scale float64) string {
				q, _, err := BuildMetricsRangeQuery(req(scale))
				if err != nil {
					t.Fatal(err)
				}
				return render(t, q)
			}
			instantSQL := func(scale float64) string {
				q, _, err := BuildMetricsInstantQuery(req(scale))
				if err != nil {
					t.Fatal(err)
				}
				return render(t, q)
			}
			for _, q := range []struct {
				build         func(float64) string
				plain, scaled string
			}{
				{rangeSQL, tc.plain, tc.scaledRange},
				{instantSQL, tc.instantPlain, tc.scaledInstant},
			} {
				res := q.build(1)
				if !strings.Contains(res, q.plain+" as value") {
					t.Errorf("scale 1 misses %q:\n%s", q.plain, res)
				}
				if zero := q.build(0); zero != res {
					t.Errorf("an unset Scale differs from Scale 1:\n%s\n%s", zero, res)
				}
				if res = q.build(4); !strings.Contains(res, q.scaled+" as value") {
					t.Errorf("scale 4 misses %q:\n%s", q.scaled, res)
				}
			}
		})
	}
}

// rate's divisor keeps its digits when the scale folds it below %f's precision.
func TestRateFoldsATinyDivisor(t *testing.T) {
	q, _, err := BuildMetricsRangeQuery(&MetricsRequest{Spans: testSpans(), Fn: "rate",
		FromNS: 10e9, ToNS: 20e9, StepNS: 1e6, AttrsTable: "attrs", Scale: 1e4})
	if err != nil {
		t.Fatal(err)
	}
	if res := render(t, q); !strings.Contains(res, "toFloat64(count(*)) / 1e-07 as value") {
		t.Errorf("divisor lost its digits:\n%s", res)
	}
}

func TestExemplarsAreNotScaled(t *testing.T) {
	req := func(fn, attr string, scale float64) *MetricsRequest {
		return &MetricsRequest{Spans: testSpans(), Fn: fn, Attr: attr,
			FromNS: 10e9, ToNS: 20e9, StepNS: 1e9, AttrsTable: "attrs", Scale: scale}
	}
	generic := func(scale float64) string {
		q, _, err := BuildGenericExemplarsQuery(req("rate", "", scale))
		if err != nil {
			t.Fatal(err)
		}
		return render(t, q)
	}
	histogram := func(scale float64) string {
		q, err := BuildHistogramExemplarsQuery(req("histogram_over_time", "duration", scale))
		if err != nil {
			t.Fatal(err)
		}
		return render(t, q)
	}
	if generic(4) != generic(1) {
		t.Errorf("generic exemplars change with Scale:\n%s", generic(4))
	}
	if histogram(4) != histogram(1) {
		t.Errorf("histogram exemplars change with Scale:\n%s", histogram(4))
	}
}

// The histogram scales its heights and leaves its bounds alone.
func TestHistogramScalesHeights(t *testing.T) {
	req := func(scale float64) *MetricsRequest {
		return &MetricsRequest{Spans: testSpans(), Fn: "histogram_over_time", Attr: "duration",
			FromNS: 10e9, ToNS: 20e9, StepNS: 1e9, AttrsTable: "attrs", Scale: scale}
	}
	build := func(scale float64) string {
		q, err := BuildHistogramRangeQuery(req(scale))
		if err != nil {
			t.Fatal(err)
		}
		return render(t, q)
	}
	plain := build(1)
	if !strings.Contains(plain, "count(*) as cnt") {
		t.Errorf("scale 1 height:\n%s", plain)
	}
	if build(0) != plain {
		t.Errorf("an unset Scale differs from Scale 1:\n%s", build(0))
	}
	scaled := build(4)
	if !strings.Contains(scaled, "(count(*)) * 4 as cnt") {
		t.Errorf("height is not scaled:\n%s", scaled)
	}
	if strings.ReplaceAll(scaled, "(count(*)) * 4 as cnt", "count(*) as cnt") != plain {
		t.Errorf("scale touches more than the height:\n%s\n%s", scaled, plain)
	}

	generic := func(scale float64) string {
		q, _, err := BuildMetricsRangeQuery(req(scale))
		if err != nil {
			t.Fatal(err)
		}
		return render(t, q)
	}
	if res := generic(1); !strings.Contains(res, "histogram(20)(main.duration_ns) as value") {
		t.Errorf("scale 1 tuple histogram:\n%s", res)
	}
	want := "arrayMap(b -> (b.1, b.2, b.3 * 4), histogram(20)(main.duration_ns)) as value"
	if res := generic(4); !strings.Contains(res, want) {
		t.Errorf("tuple histogram misses %q:\n%s", want, res)
	}
}

// compare() scales both counts where they are counted, so top-N and final
// order read the scaled aliases unchanged.
func TestCompareScalesBothCounts(t *testing.T) {
	spans := sql.NewWith(sql.NewSelect().Select(sql.NewRawObject("trace_id")).From(sql.NewRawObject("b")), "baseline_spans")
	req := func(scale float64) *CompareRequest {
		return &CompareRequest{Baseline: spans, FromNS: 100e9, ToNS: 200e9,
			BaselineFromNS: 50e9, BaselineToNS: 150e9, AttrsTable: "attrs", TopN: 3, Scale: scale}
	}
	const (
		sel    = "countIf(sel.trace_id != '') as selection_count"
		base   = "countIf(sel.trace_id = '') as baseline_count"
		rank   = "row_number() OVER (PARTITION BY attr_key ORDER BY (selection_count + baseline_count) DESC) as rn"
		order  = "ORDER BY attr_key asc , selection_count + baseline_count desc"
		scaleS = "(countIf(sel.trace_id != '')) * 4 as selection_count"
		scaleB = "(countIf(sel.trace_id = '')) * 4 as baseline_count"
	)
	plain := render(t, mustCompare(t, req(1)))
	for _, want := range []string{sel, base, rank, order} {
		if !strings.Contains(plain, want) {
			t.Errorf("scale 1 misses %q:\n%s", want, plain)
		}
	}
	if zero := render(t, mustCompare(t, req(0))); zero != plain {
		t.Errorf("an unset Scale differs from Scale 1:\n%s", zero)
	}
	scaled := render(t, mustCompare(t, req(4)))
	for _, want := range []string{scaleS, scaleB, rank, order} {
		if !strings.Contains(scaled, want) {
			t.Errorf("scale 4 misses %q:\n%s", want, scaled)
		}
	}
	unscaled := strings.NewReplacer(scaleS, sel, scaleB, base).Replace(scaled)
	if unscaled != plain {
		t.Errorf("scale touches more than the counts:\n%s\n%s", scaled, plain)
	}
}
