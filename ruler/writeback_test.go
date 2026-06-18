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

func TestVectorToWriteRequest_EmptyVector(t *testing.T) {
	wr := vectorToWriteRequest("r", nil, promql.Vector{})
	if len(wr.GetTimeseries()) != 0 {
		t.Fatalf("expected no series for empty vector, got %d", len(wr.GetTimeseries()))
	}
}
