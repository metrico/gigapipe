package metriccache

import (
	"unsafe"

	"github.com/VictoriaMetrics/fastcache"
)

const fingerprintCacheBytes = 100 * 1024 * 1024

// Fingerprints is the fingerprint cache: the series whose metric_series row
// is written, until the next Reset.
type Fingerprints struct {
	seen *fastcache.Cache
}

func NewFingerprints() *Fingerprints {
	return &Fingerprints{seen: fastcache.New(fingerprintCacheBytes)}
}

// FirstSight reports whether fp is unknown, and records it. Concurrent first
// sights of one series may both report true.
func (f *Fingerprints) FirstSight(fp uint64) bool {
	k := unsafe.Slice((*byte)(unsafe.Pointer(&fp)), 8)
	if f.seen.Has(k) {
		return false
	}
	f.seen.Set(k, nil)
	return true
}

func (f *Fingerprints) Reset() {
	f.seen.Reset()
}
