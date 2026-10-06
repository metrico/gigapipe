package model

import (
	"math"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
)

type Labels []labels.Label

type ILabelsGetter interface {
	Get(fp uint64) Labels
	GetNative(fp uint64) labels.Labels
}

var _ storage.SeriesSet = &SeriesSet{}

type SeriesSet struct {
	Error  error
	Series []*SeriesV2
	idx    int
}

func (e *SeriesSet) Reset() {
	e.idx = -1
}

func (e *SeriesSet) Err() error {
	return e.Error
}

func (e *SeriesSet) Next() bool {
	e.idx++
	return e.Series != nil && e.idx < len(e.Series)
}

func (e *SeriesSet) At() storage.Series {
	return e.Series[e.idx]
}

func (e *SeriesSet) Warnings() annotations.Annotations {
	return nil
}

type Sample struct {
	TimestampMs int64
	Value       float64
}

var _ storage.Series = &SeriesV2{}

type SeriesV2 struct {
	LabelsGetter ILabelsGetter
	Fp           uint64
	Samples      []Sample
}

func (s *SeriesV2) Labels() labels.Labels {
	return s.LabelsGetter.GetNative(s.Fp)
}

func (s *SeriesV2) LabelsArray() Labels {
	return s.LabelsGetter.Get(s.Fp)
}

func (s *SeriesV2) Iterator(it chunkenc.Iterator) chunkenc.Iterator {
	return &seriesIt{
		samples: s.Samples,
		idx:     -1,
	}
}

var _ chunkenc.Iterator = &seriesIt{}

type seriesIt struct {
	samples []Sample
	idx     int
}

func (s *seriesIt) Next() chunkenc.ValueType {
	s.idx++
	if s.idx < len(s.samples) {
		return chunkenc.ValFloat
	}
	return chunkenc.ValNone
}

func (s *seriesIt) Seek(t int64) chunkenc.ValueType {
	l := 0
	u := len(s.samples)
	idx := int(0)
	if t <= s.samples[0].TimestampMs {
		s.idx = 0
		return chunkenc.ValFloat
	}
	for u > l {
		idx = (u + l) / 2
		if s.samples[idx].TimestampMs == t {
			l = idx
			break
		}
		if s.samples[idx].TimestampMs < t {
			l = idx + 1
			continue
		}
		u = idx
	}
	s.idx = idx
	if s.idx < len(s.samples) {
		return chunkenc.ValFloat
	}
	return chunkenc.ValNone
}

func (s *seriesIt) At() (int64, float64) {
	return s.samples[s.idx].TimestampMs, s.samples[s.idx].Value
}

func (s *seriesIt) AtHistogram(histogram *histogram.Histogram) (int64, *histogram.Histogram) {
	return 0, nil
}

func (s *seriesIt) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return 0, nil
}

func (s *seriesIt) AtT() int64 {
	return s.samples[s.idx].TimestampMs
}

func (s *seriesIt) AtST() int64 {
	return 0
}

func (s *seriesIt) Err() error {
	return nil
}

// StaleMarkerValue is the Prometheus stale marker encoded as a float64. The
// PromQL engine detects it via value.IsStaleNaN and stops carrying a series
// forward at that timestamp, regardless of its LookbackDelta. It is the single
// source of truth for the marker across the reader.
var StaleMarkerValue = math.Float64frombits(value.StaleNaN)
