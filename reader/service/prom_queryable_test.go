package service

import (
	"testing"

	"github.com/metrico/qryn/v5/reader/model"
	"github.com/prometheus/prometheus/model/labels"
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
