package model

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"sync"
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
	Prolong      bool
	StepMs       int64
}

func (s *SeriesV2) Labels() labels.Labels {
	return s.LabelsGetter.GetNative(s.Fp)
}

func (s *SeriesV2) LabelsArray() Labels {
	return s.LabelsGetter.Get(s.Fp)
}

func (s *SeriesV2) Iterator(it chunkenc.Iterator) chunkenc.Iterator {
	if !s.Prolong {
		return &seriesIt{
			samples: s.Samples,
			idx:     -1,
		}
	}
	return &prolongSeriesIt{
		s: &seriesIt{
			samples: s.Samples,
			idx:     -1,
		},
		stepMs: s.StepMs,
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
	return 0
}

func (s *seriesIt) Err() error {
	return nil
}

type prolongSeriesIt struct {
	s           *seriesIt
	prev        *Sample
	next        *Sample
	stepMs      int64
	timestampMs int64
	m           sync.Mutex
}

func (p *prolongSeriesIt) Err() error {
	return p.s.Err()
}

func (p *prolongSeriesIt) Seek(t int64) chunkenc.ValueType {
	p.m.Lock()
	defer p.m.Unlock()
	p.prev = nil
	p.next = nil
	if p.s.Seek(t) == chunkenc.ValNone {
		return chunkenc.ValNone
	}
	p.prev = &p.s.samples[p.s.idx]
	p.timestampMs = p.prev.TimestampMs
	if p.s.Next() != chunkenc.ValNone {
		p.next = &p.s.samples[p.s.idx]
	}
	return chunkenc.ValFloat
}

func (p *prolongSeriesIt) Next() chunkenc.ValueType {
	p.m.Lock()
	defer p.m.Unlock()
	if p.prev == nil {
		if p.s.Next() == chunkenc.ValNone {
			return chunkenc.ValNone
		}
		p.prev = &p.s.samples[p.s.idx]
		if p.s.Next() != chunkenc.ValNone {
			p.next = &p.s.samples[p.s.idx]
		}
		p.timestampMs = p.prev.TimestampMs
		return chunkenc.ValFloat
	}

	if p.next == nil {
		if p.timestampMs > p.prev.TimestampMs+300000 {
			return chunkenc.ValNone
		}
		p.timestampMs += p.stepMs
		return chunkenc.ValFloat
	}

	done := false
	for p.timestampMs > p.prev.TimestampMs+300000 || (p.next != nil && p.timestampMs >= p.next.TimestampMs) {
		p.prev = p.next
		p.next = nil
		if p.s.Next() != chunkenc.ValNone {
			p.next = &p.s.samples[p.s.idx]
		}
		p.timestampMs = p.prev.TimestampMs
		done = true
	}

	if done {
		return chunkenc.ValFloat
	}

	p.timestampMs += p.stepMs
	return chunkenc.ValFloat
}

func (p *prolongSeriesIt) At() (int64, float64) {
	return p.timestampMs, p.prev.Value
}

// AtHistogram returns the current timestamp/value pair if the value is a
// histogram with integer counts. Before the iterator has advanced, the behaviour
// is unspecified.
// The method accepts an optional Histogram object which will be
// reused when not nil. Otherwise, a new Histogram object will be allocated.
func (s *prolongSeriesIt) AtHistogram(histogram *histogram.Histogram) (int64, *histogram.Histogram) {
	return 0, nil
}

// AtFloatHistogram returns the current timestamp/value pair if the
// value is a histogram with floating-point counts. It also works if the
// value is a histogram with integer counts, in which case a
// FloatHistogram copy of the histogram is returned. Before the iterator
// has advanced, the behaviour is unspecified.
// The method accepts an optional FloatHistogram object which will be
// reused when not nil. Otherwise, a new FloatHistogram object will be allocated.
func (s *prolongSeriesIt) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return 0, nil
}

// AtT returns the current timestamp.
// Before the iterator has advanced, the behaviour is unspecified.
func (s *prolongSeriesIt) AtT() int64 {
	return 0
}
