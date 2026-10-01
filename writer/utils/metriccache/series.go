package metriccache

import (
	"sync"
	"unsafe"

	"github.com/VictoriaMetrics/fastcache"
)

const seriesCacheBytes = 100 * 1024 * 1024

// Metadata is a metric family's type, help and unit.
type Metadata struct {
	Type string
	Help string
	Unit string
}

// Series remembers the series and family metadata already written, until its
// next Reset.
type Series struct {
	seen *fastcache.Cache
	mtx  sync.Mutex
	meta map[string]Metadata
}

func NewSeries() *Series {
	return &Series{
		seen: fastcache.New(seriesCacheBytes),
		meta: make(map[string]Metadata),
	}
}

// FirstSight reports whether fp is unknown, and records it.
func (s *Series) FirstSight(fp uint64) bool {
	k := unsafe.Slice((*byte)(unsafe.Pointer(&fp)), 8)
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if s.seen.Has(k) {
		return false
	}
	s.seen.Set(k, nil)
	return true
}

// MetadataChanged reports whether m differs from the family's recorded
// metadata, and records it.
func (s *Series) MetadataChanged(name string, m Metadata) bool {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if old, ok := s.meta[name]; ok && old == m {
		return false
	}
	s.meta[name] = m
	return true
}

// Reset forgets every series and every family's metadata.
func (s *Series) Reset() {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.seen.Reset()
	s.meta = make(map[string]Metadata)
}
