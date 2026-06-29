package ruler

import (
	"testing"

	"github.com/metrico/qryn/v4/writer/utils/proto/prompb"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
)

func labelMap(ls []*prompb.Label) map[string]string {
	m := make(map[string]string, len(ls))
	for _, l := range ls {
		m[l.GetName()] = l.GetValue()
	}
	return m
}

func TestVectorToWriteRequest_NamesSeriesAndMergesLabels(t *testing.T) {
	v := promql.Vector{
		{
			T:      1700000000000,
			F:      42,
			Metric: labels.FromStrings("__name__", "http_requests_total", "instance", "a"),
		},
	}
	ruleLabels := map[string]string{"team": "infra"}

	wr := vectorToWriteRequest("job:http:rate5m", ruleLabels, v)

	if len(wr.GetTimeseries()) != 1 {
		t.Fatalf("expected 1 series, got %d", len(wr.GetTimeseries()))
	}
	ts := wr.GetTimeseries()[0]

	got := labelMap(ts.GetLabels())
	want := map[string]string{
		"__name__": "job:http:rate5m", // record name replaces the source metric name
		"team":     "infra",           // rule labels are included
		"instance": "a",               // sample labels are carried through
	}
	for k, wv := range want {
		if got[k] != wv {
			t.Errorf("label %q = %q, want %q", k, got[k], wv)
		}
	}
	if _, ok := got["http_requests_total"]; ok {
		t.Errorf("source __name__ leaked as a label: %v", got)
	}

	if len(ts.GetSamples()) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(ts.GetSamples()))
	}
	s := ts.GetSamples()[0]
	if s.GetValue() != 42 {
		t.Errorf("value = %v, want 42", s.GetValue())
	}
	if s.GetTimestamp() != 1700000000000 {
		t.Errorf("timestamp = %d, want 1700000000000 (ms)", s.GetTimestamp())
	}
}

func TestVectorToWriteRequest_RuleLabelsOverrideSampleLabels(t *testing.T) {
	v := promql.Vector{
		{
			T:      1700000000000,
			F:      1,
			Metric: labels.FromStrings("__name__", "up", "job", "api", "instance", "a"),
		},
	}
	ruleLabels := map[string]string{"job": "aggregator"}

	wr := vectorToWriteRequest("job:up:count", ruleLabels, v)

	ts := wr.GetTimeseries()[0]

	// Each label name appears exactly once; a colliding key must not be emitted twice.
	counts := make(map[string]int)
	for _, l := range ts.GetLabels() {
		counts[l.GetName()]++
	}
	for name, n := range counts {
		if n != 1 {
			t.Errorf("label %q emitted %d times, want 1", name, n)
		}
	}

	got := labelMap(ts.GetLabels())
	if got["job"] != "aggregator" {
		t.Errorf("job = %q, want %q (rule label overrides sample label)", got["job"], "aggregator")
	}
	if got["__name__"] != "job:up:count" {
		t.Errorf("__name__ = %q, want %q", got["__name__"], "job:up:count")
	}
	if got["instance"] != "a" {
		t.Errorf("instance = %q, want %q (non-colliding sample label preserved)", got["instance"], "a")
	}
}

func TestVectorToWriteRequest_EmptyVector(t *testing.T) {
	wr := vectorToWriteRequest("r", nil, promql.Vector{})
	if len(wr.GetTimeseries()) != 0 {
		t.Fatalf("expected no series for empty vector, got %d", len(wr.GetTimeseries()))
	}
}
