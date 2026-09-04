package model

import (
	"math"
	"sync"

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
		stepMs:  s.StepMs,
		staleAt: -1,
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

// prolongSeriesIt terminates a run of samples with a Prometheus stale marker at
// the lookback boundary instead of forward-filling the last value on the step
// grid for 5 minutes.
//
// For an instant vector selector (or rate/deriv/delta) in a range query, the
// Prometheus engine carries the last real sample forward for its LookbackDelta
// (5m by default). gigapipe used to *also* forward-fill for 5m inside this
// iterator, so a sample at T could stay visible until ~T+10m (issue #931).
//
// This iterator instead yields the real samples untouched and, when a run ends,
// emits a single stale marker at lastSample + LookbackDeltaMs. The engine
// carries the last value forward across its own lookback window and the marker
// terminates it exactly at the boundary, so total visibility is ~5m - matching
// upstream Prometheus and never stacking to ~10m.
//
// A run ends when the next real sample is more than LookbackDeltaMs away, or
// when there are no more samples. Gaps up to LookbackDeltaMs are left unmarked
// and bridged by the engine's lookback, so genuinely *sparse* series behave the
// same as in upstream Prometheus. If a sparse series that was marked stale
// later resumes (its next sample lands more than LookbackDeltaMs after the
// previous one), the marker sits in the gap between the two real samples: the
// old value stops at the boundary and the new value simply starts a fresh run -
// exactly what Prometheus does when a series reappears after going stale.
type prolongSeriesIt struct {
	s           *seriesIt
	stepMs      int64
	timestampMs int64
	value       float64
	// staleAt, when set (>= 0), is the timestamp at which the next Next() must
	// emit a stale marker before advancing to the following real sample.
	staleAt int64
	m       sync.Mutex
}

func (p *prolongSeriesIt) Err() error {
	return p.s.Err()
}

// StaleMarkerValue is the Prometheus stale marker encoded as a float64. The
// PromQL engine detects it via value.IsStaleNaN and stops carrying a series
// forward at that timestamp, regardless of its LookbackDelta. It is the single
// source of truth for the marker across the reader.
var StaleMarkerValue = math.Float64frombits(value.StaleNaN)

// LookbackDeltaMs mirrors the Prometheus engine's default lookback delta (5m,
// the value NewPromEngine relies on). A sample stays valid for at most this
// long, so a gap larger than this is where a series is considered to have
// stopped and a stale marker is emitted.
const LookbackDeltaMs = int64(5 * 60 * 1000)

// scheduleStaleAfterCurrent decides whether the sample currently under p.s (at
// p.s.idx) ends a run and, if so, schedules a stale marker.
//
// A run ends when the following real sample is more than LookbackDeltaMs away
// (a gap the engine's lookback would not bridge) or when there is no following
// sample. In that case the marker is placed at cur + LookbackDeltaMs: the last
// real value stays visible for the full lookback window - exactly as upstream
// Prometheus carries a sample forward - and the marker then terminates it at
// the lookback boundary, so the engine cannot extend it any further. Gaps up to
// LookbackDeltaMs are left unmarked and bridged by the engine's own lookback,
// matching Prometheus behavior for sparse series.
//
// Because a marker is only scheduled when the next sample is more than
// LookbackDeltaMs away, cur + LookbackDeltaMs is always strictly before that
// next sample, so markers never collide with or reorder real samples.
func (p *prolongSeriesIt) scheduleStaleAfterCurrent() {
	cur := p.s.samples[p.s.idx].TimestampMs
	nextIdx := p.s.idx + 1
	if nextIdx < len(p.s.samples) && p.s.samples[nextIdx].TimestampMs <= cur+LookbackDeltaMs {
		// The next sample is within the lookback window; the engine bridges the
		// gap on its own. No marker.
		return
	}
	p.staleAt = cur + LookbackDeltaMs
}

func (p *prolongSeriesIt) Seek(t int64) chunkenc.ValueType {
	p.m.Lock()
	defer p.m.Unlock()
	p.staleAt = -1
	if p.s.Seek(t) == chunkenc.ValNone {
		return chunkenc.ValNone
	}
	p.timestampMs = p.s.samples[p.s.idx].TimestampMs
	p.value = p.s.samples[p.s.idx].Value
	p.scheduleStaleAfterCurrent()
	return chunkenc.ValFloat
}

func (p *prolongSeriesIt) Next() chunkenc.ValueType {
	p.m.Lock()
	defer p.m.Unlock()

	// A stale marker was scheduled after the previous real sample: emit it now.
	if p.staleAt >= 0 {
		p.timestampMs = p.staleAt
		p.value = StaleMarkerValue
		p.staleAt = -1
		return chunkenc.ValFloat
	}

	if p.s.Next() == chunkenc.ValNone {
		return chunkenc.ValNone
	}
	p.timestampMs = p.s.samples[p.s.idx].TimestampMs
	p.value = p.s.samples[p.s.idx].Value
	p.scheduleStaleAfterCurrent()
	return chunkenc.ValFloat
}

func (p *prolongSeriesIt) At() (int64, float64) {
	return p.timestampMs, p.value
}

// AtHistogram returns the current timestamp/value pair if the value is a
// histogram with integer counts. Before the iterator has advanced, the behaviour
// is unspecified.
// The method accepts an optional Histogram object which will be
// reused when not nil. Otherwise, a new Histogram object will be allocated.
func (p *prolongSeriesIt) AtHistogram(histogram *histogram.Histogram) (int64, *histogram.Histogram) {
	return 0, nil
}

// AtFloatHistogram returns the current timestamp/value pair if the
// value is a histogram with floating-point counts. It also works if the
// value is a histogram with integer counts, in which case a
// FloatHistogram copy of the histogram is returned. Before the iterator
// has advanced, the behaviour is unspecified.
// The method accepts an optional FloatHistogram object which will be
// reused when not nil. Otherwise, a new FloatHistogram object will be allocated.
func (p *prolongSeriesIt) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return 0, nil
}

// AtT returns the current timestamp.
// Before the iterator has advanced, the behaviour is unspecified.
func (p *prolongSeriesIt) AtT() int64 {
	return p.timestampMs
}

// AtST returns the current start timestamp.
func (p *prolongSeriesIt) AtST() int64 {
	return 0
}
