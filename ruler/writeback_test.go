package ruler

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
)

func TestRecordedVector_NamesSeriesMergesLabelsAndStampsTheTick(t *testing.T) {
	v := promql.Vector{{
		T:      1700000000123,
		F:      42,
		Metric: labels.FromStrings("__name__", "http_requests_total", "instance", "a"),
	}}

	got, err := recordedVector("job:http:rate5m", map[string]string{"team": "infra"}, v, 1700000000000)
	if err != nil {
		t.Fatal(err)
	}

	want := labels.FromStrings("__name__", "job:http:rate5m", "instance", "a", "team", "infra")
	if len(got) != 1 {
		t.Fatalf("got %d samples, want 1", len(got))
	}
	if !labels.Equal(got[0].Metric, want) {
		t.Errorf("labels = %s, want %s", got[0].Metric, want)
	}
	if got[0].F != 42 || got[0].T != 1700000000000 {
		t.Errorf("sample = (%d, %v), want (1700000000000, 42)", got[0].T, got[0].F)
	}
}

func TestRecordedVector_RuleLabelsOverrideSampleLabels(t *testing.T) {
	v := promql.Vector{{
		T:      1700000000000,
		F:      1,
		Metric: labels.FromStrings("__name__", "up", "job", "api", "instance", "a"),
	}}

	got, err := recordedVector("job:up:count", map[string]string{"job": "aggregator", "__name__": "x"}, v, 1700000000000)
	if err != nil {
		t.Fatal(err)
	}

	want := labels.FromStrings("__name__", "job:up:count", "instance", "a", "job", "aggregator")
	if !labels.Equal(got[0].Metric, want) {
		t.Errorf("labels = %s, want %s", got[0].Metric, want)
	}
}

func TestRecordedVector_EmptyVector(t *testing.T) {
	got, err := recordedVector("r", nil, promql.Vector{}, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d samples for an empty vector, want 0", len(got))
	}
}

func TestRecordedVector_UnlabelledSampleGetsTheRecordName(t *testing.T) {
	v := promql.Vector{{T: 5, F: 7, Metric: labels.EmptyLabels()}}

	got, err := recordedVector("logs:errors:count", nil, v, 1000)
	if err != nil {
		t.Fatal(err)
	}

	if want := labels.FromStrings("__name__", "logs:errors:count"); !labels.Equal(got[0].Metric, want) {
		t.Errorf("labels = %s, want %s", got[0].Metric, want)
	}
	if got[0].T != 1000 || got[0].F != 7 {
		t.Errorf("sample = (%d, %v), want (1000, 7)", got[0].T, got[0].F)
	}
}

func TestRecordedVector_RejectsALabelSetRepeatedByRuleLabels(t *testing.T) {
	v := promql.Vector{
		{F: 1, Metric: labels.FromStrings("job", "a")},
		{F: 2, Metric: labels.FromStrings("job", "b")},
	}

	if _, err := recordedVector("r", map[string]string{"job": "x"}, v, 1000); err == nil {
		t.Fatal("expected an error for two samples with one label set after rule labels")
	}
}
