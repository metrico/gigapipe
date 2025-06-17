package model

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"sync"
)

type ILabelsGetter interface {
	Get(fp uint64) labels.Labels
}

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

func (e *SeriesSet) Warnings() storage.Warnings {
	return nil
}

type Sample struct {
	TimestampMs int64
	Value       float64
}

type SeriesV2 struct {
	LabelsGetter ILabelsGetter
	Fp           uint64
	Samples      []Sample
	Prolong      bool
	StepMs       int64
}

func (s *SeriesV2) Labels() labels.Labels {
	return s.LabelsGetter.Get(s.Fp)
}

func (s *SeriesV2) Iterator() chunkenc.Iterator {
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

type seriesIt struct {
	samples []Sample
	idx     int
}

func (s *seriesIt) Next() bool {
	s.idx++
	return s.idx < len(s.samples)
}

func (s *seriesIt) Seek(t int64) bool {
	l := 0
	u := len(s.samples)
	idx := int(0)
	if t <= s.samples[0].TimestampMs {
		s.idx = 0
		return true
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
	return s.idx < len(s.samples)
}

func (s *seriesIt) At() (int64, float64) {
	return s.samples[s.idx].TimestampMs, s.samples[s.idx].Value
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

func (p *prolongSeriesIt) Seek(t int64) bool {
	p.m.Lock()
	defer p.m.Unlock()
	p.prev = nil
	p.next = nil
	if !p.s.Seek(t) {
		return false
	}
	p.prev = &p.s.samples[p.s.idx]
	p.timestampMs = p.prev.TimestampMs
	if p.s.Next() {
		p.next = &p.s.samples[p.s.idx]
	}
	return true
}

func (p *prolongSeriesIt) Next() bool {
	p.m.Lock()
	defer p.m.Unlock()
	if p.prev == nil {
		if !p.s.Next() {
			return false
		}
		p.prev = &p.s.samples[p.s.idx]
		if p.s.Next() {
			p.next = &p.s.samples[p.s.idx]
		}
		p.timestampMs = p.prev.TimestampMs
		return true
	}

	if p.next == nil {
		if p.timestampMs > p.prev.TimestampMs+300000 {
			return false
		}
		p.timestampMs += p.stepMs
		return true
	}

	done := false
	for p.timestampMs > p.prev.TimestampMs+300000 || (p.next != nil && p.timestampMs >= p.next.TimestampMs) {
		p.prev = p.next
		p.next = nil
		if p.s.Next() {
			p.next = &p.s.samples[p.s.idx]
		}
		p.timestampMs = p.prev.TimestampMs
		done = true
	}

	if done {
		return true
	}

	p.timestampMs += p.stepMs
	return true
}

func (p *prolongSeriesIt) At() (int64, float64) {
	return p.timestampMs, p.prev.Value
}
