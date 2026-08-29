package service

import (
	"testing"

	"github.com/metrico/qryn/v5/reader/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
)

// stubLabelsGetter is a minimal model.ILabelsGetter backed by a map, for
// testing ReshuffleSeries without needing a real ClickHouse-backed getter.
type stubLabelsGetter struct {
	byFp map[uint64]model.Labels
}

func (s *stubLabelsGetter) Get(fp uint64) model.Labels {
	return s.byFp[fp]
}

func (s *stubLabelsGetter) GetNative(fp uint64) labels.Labels {
	return labels.Labels{}
}

func mkLabels(pairs ...string) model.Labels {
	lbls := make(model.Labels, 0, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		lbls = append(lbls, labels.Label{Name: pairs[i], Value: pairs[i+1]})
	}
	return lbls
}

func TestReshuffleSeries_SameFpSameLabels(t *testing.T) {
	getter := &stubLabelsGetter{byFp: map[uint64]model.Labels{
		1: mkLabels("__name__", "foo", "job", "bar"),
	}}
	s1 := &model.SeriesV2{
		LabelsGetter: getter,
		Fp:           1,
		Samples: []model.Sample{
			{TimestampMs: 20, Value: 2},
			{TimestampMs: 40, Value: 4},
		},
	}
	s2 := &model.SeriesV2{
		LabelsGetter: getter,
		Fp:           1,
		Samples: []model.Sample{
			{TimestampMs: 10, Value: 1},
			{TimestampMs: 30, Value: 3},
		},
	}
	c := &CLokiQuerier{}
	out := c.ReshuffleSeries([]*model.SeriesV2{s1, s2})

	if len(out) != 1 {
		t.Fatalf("expected 1 series, got %d", len(out))
	}
	samples := out[0].Samples
	if len(samples) != 4 {
		t.Fatalf("expected 4 merged samples, got %d", len(samples))
	}
	for i := 1; i < len(samples); i++ {
		if samples[i-1].TimestampMs > samples[i].TimestampMs {
			t.Fatalf("samples not sorted ascending by TimestampMs: %+v", samples)
		}
	}
	wantTs := []int64{10, 20, 30, 40}
	for i, ts := range wantTs {
		if samples[i].TimestampMs != ts {
			t.Fatalf("sample %d: expected ts %d, got %d", i, ts, samples[i].TimestampMs)
		}
	}
}

func TestReshuffleSeries_DifferentFpSameLabels(t *testing.T) {
	getter := &stubLabelsGetter{byFp: map[uint64]model.Labels{
		1: mkLabels("__name__", "foo", "job", "bar"),
		2: mkLabels("__name__", "foo", "job", "bar"),
	}}
	s1 := &model.SeriesV2{
		LabelsGetter: getter,
		Fp:           1,
		Samples: []model.Sample{
			{TimestampMs: 10, Value: 1},
		},
	}
	s2 := &model.SeriesV2{
		LabelsGetter: getter,
		Fp:           2,
		Samples: []model.Sample{
			{TimestampMs: 20, Value: 2},
		},
	}
	c := &CLokiQuerier{}
	out := c.ReshuffleSeries([]*model.SeriesV2{s1, s2})

	if len(out) != 1 {
		t.Fatalf("expected 1 series, got %d", len(out))
	}
	if len(out[0].Samples) != 2 {
		t.Fatalf("expected 2 merged samples, got %d", len(out[0].Samples))
	}
}

func TestReshuffleSeries_DistinctLabelSets(t *testing.T) {
	getter := &stubLabelsGetter{byFp: map[uint64]model.Labels{
		1: mkLabels("__name__", "foo"),
		2: mkLabels("__name__", "bar"),
	}}
	s1 := &model.SeriesV2{
		LabelsGetter: getter,
		Fp:           1,
		Samples: []model.Sample{
			{TimestampMs: 10, Value: 1},
		},
	}
	s2 := &model.SeriesV2{
		LabelsGetter: getter,
		Fp:           2,
		Samples: []model.Sample{
			{TimestampMs: 20, Value: 2},
		},
	}
	c := &CLokiQuerier{}
	in := []*model.SeriesV2{s1, s2}
	out := c.ReshuffleSeries(in)

	if len(out) != 2 {
		t.Fatalf("expected 2 series, got %d", len(out))
	}
	if out[0] != s1 || out[1] != s2 {
		t.Fatalf("expected input order preserved")
	}
	if len(out[0].Samples) != 1 || len(out[1].Samples) != 1 {
		t.Fatalf("samples should be untouched")
	}
	if out[0].Samples[0].TimestampMs != 10 || out[1].Samples[0].TimestampMs != 20 {
		t.Fatalf("samples should be untouched")
	}
}

// The accelerated (substitute-backed) path builds a SeriesV2 with Prolong=false
// directly from ClickHouse rows. Because the SQL layer fills forward for 5
// minutes (const staleness) AND the engine applies its own 5-minute
// LookbackDelta, a value at T could remain visible until ~T+10m (issue #931).
//
// The fix appends a Prometheus stale marker one step past the last real sample
// so the engine stops carrying the value forward. These tests pin the semantics
// of the appendStaleMarker helper that Select() calls.

func lastIsStaleMarker(t *testing.T, samples []model.Sample, wantTs int64) {
	t.Helper()
	if len(samples) == 0 {
		t.Fatalf("expected samples, got none")
	}
	last := samples[len(samples)-1]
	if last.TimestampMs != wantTs {
		t.Errorf("stale marker timestamp: expected %d, got %d", wantTs, last.TimestampMs)
	}
	if !value.IsStaleNaN(last.Value) {
		t.Errorf("expected last sample to be a stale marker, got value=%v", last.Value)
	}
}

// TestAppendStaleMarker_AppendsOneStepPastLast is the core behavior: a stale
// marker is appended one step past the last real sample when that sample ends
// before the query window edge (i.e., the series genuinely stopped).
func TestAppendStaleMarker_AppendsOneStepPastLast(t *testing.T) {
	const step = int64(15000)
	const queryEnd = int64(1_000_000) // well past the last sample
	in := []model.Sample{
		{TimestampMs: 100000, Value: 1},
		{TimestampMs: 115000, Value: 2},
		{TimestampMs: 130000, Value: 3},
	}

	out := appendStaleMarker(in, step, queryEnd)

	if len(out) != len(in)+1 {
		t.Fatalf("expected %d samples (3 real + 1 marker), got %d", len(in)+1, len(out))
	}
	// Real samples untouched.
	for i := range in {
		if out[i] != in[i] {
			t.Fatalf("real sample %d changed: %+v -> %+v", i, in[i], out[i])
		}
	}
	lastIsStaleMarker(t, out, 130000+step)
}

// TestAppendStaleMarker_NoMarkerAtQueryEdge verifies we do NOT emit a stale
// marker for a series that is still live at the query window edge. Prometheus
// only stale-marks a series that actually stops, not one that is simply
// truncated by the end of the query range.
func TestAppendStaleMarker_NoMarkerAtQueryEdge(t *testing.T) {
	const step = int64(15000)
	// Last sample is within one step of the query end -> still live.
	const queryEnd = int64(130000 + 5000)
	in := []model.Sample{
		{TimestampMs: 100000, Value: 1},
		{TimestampMs: 130000, Value: 3},
	}

	out := appendStaleMarker(in, step, queryEnd)

	if len(out) != len(in) {
		t.Fatalf("expected no marker at the query edge, got %d samples: %+v", len(out), out)
	}
	for _, s := range out {
		if value.IsStaleNaN(s.Value) {
			t.Fatalf("unexpected stale marker for a series live at the query edge: %+v", out)
		}
	}
}

// TestAppendStaleMarker_EmptySeries verifies no marker and no panic for empty
// input.
func TestAppendStaleMarker_EmptySeries(t *testing.T) {
	out := appendStaleMarker(nil, 15000, 1_000_000)
	if len(out) != 0 {
		t.Fatalf("expected empty output for empty input, got %+v", out)
	}
}

// TestAppendStaleMarker_ZeroStepNoop verifies that with an unknown/zero step we
// do not fabricate a marker at a bogus timestamp.
func TestAppendStaleMarker_ZeroStepNoop(t *testing.T) {
	in := []model.Sample{{TimestampMs: 100000, Value: 1}}
	out := appendStaleMarker(in, 0, 1_000_000)
	if len(out) != len(in) {
		t.Fatalf("expected no marker with zero step, got %+v", out)
	}
}
