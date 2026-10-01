// Package metriccache holds the writer's per-node metric caches: the
// predecessor cache behind each staging row and the series and metadata caches
// that gate metric_series and metric_metadata rows.
package metriccache

import (
	"math"
	"sync"
	"time"
)

// staleMarkerBits is the bit pattern of the Prometheus stale marker NaN.
const staleMarkerBits uint64 = 0x7ff0000000000002

// isStaleMarker reports whether v is the stale marker, bit for bit.
func isStaleMarker(v float64) bool {
	return math.Float64bits(v) == staleMarkerBits
}

// Prev is a staging row's predecessor columns. TimestampMs 0 means unknown.
type Prev struct {
	TimestampMs int64
	Value       float64
	Aggregate   uint8
}

const predShards = 64

type predEntry struct {
	tsMs   int64
	value  float64
	seenAt int64
}

type predShard struct {
	mtx     sync.Mutex
	entries map[uint64]predEntry
}

// Predecessors maps a series fingerprint to the last non-stale sample the
// writer accepted for it.
type Predecessors struct {
	shards [predShards]predShard
	idle   time.Duration
	now    func() time.Time
}

func NewPredecessors(idle time.Duration, now func() time.Time) *Predecessors {
	if now == nil {
		now = time.Now
	}
	p := &Predecessors{idle: idle, now: now}
	for i := range p.shards {
		p.shards[i].entries = make(map[uint64]predEntry)
	}
	return p
}

// Next returns the predecessor of the sample (fp, tsMs, value) and advances
// the series to it when the sample is later than the predecessor and not a
// stale marker.
func (p *Predecessors) Next(fp uint64, tsMs int64, value float64) Prev {
	s := &p.shards[fp%predShards]
	now := p.now().UnixNano()
	s.mtx.Lock()
	defer s.mtx.Unlock()
	e, ok := s.entries[fp]
	var prev Prev
	if ok {
		prev = Prev{TimestampMs: e.tsMs, Value: e.value}
	}
	if !ok || tsMs > e.tsMs {
		prev.Aggregate = 1
		if !isStaleMarker(value) {
			s.entries[fp] = predEntry{tsMs: tsMs, value: value, seenAt: now}
			return prev
		}
	}
	if ok {
		e.seenAt = now
		s.entries[fp] = e
	}
	return prev
}

// EvictIdle drops the series without a sample for the idle period.
func (p *Predecessors) EvictIdle() {
	cutoff := p.now().Add(-p.idle).UnixNano()
	for i := range p.shards {
		s := &p.shards[i]
		s.mtx.Lock()
		for fp, e := range s.entries {
			if e.seenAt <= cutoff {
				delete(s.entries, fp)
			}
		}
		s.mtx.Unlock()
	}
}
