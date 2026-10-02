package metriccache

import (
	"encoding/binary"
	"sync"
	"time"
	"unsafe"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/metrico/qryn/v5/shared/metricindex"
)

const (
	fingerprintCacheBytes = 100 * 1024 * 1024
	fingerprintStripes    = 256
	seriesIndexLagMs      = int64(metricindex.SeriesIndexLag / time.Millisecond)
)

// Fingerprints is the fingerprint cache: per series, the (first_seen,
// last_seen) of the last metric_series row emitted since the last Reset.
type Fingerprints struct {
	seen    *fastcache.Cache
	stripes [fingerprintStripes]sync.Mutex
}

func NewFingerprints() *Fingerprints {
	return &Fingerprints{seen: fastcache.New(fingerprintCacheBytes)}
}

// Emit reports whether samples of fp spanning [minMs, maxMs] need a
// metric_series row, and returns that row's bounds: the union of the last
// emitted row and the samples. A row is needed on first sight, below the
// emitted first_seen, or more than SeriesIndexLag above the emitted last_seen.
func (f *Fingerprints) Emit(fp uint64, minMs, maxMs int64) (firstMs, lastMs int64, ok bool) {
	k := unsafe.Slice((*byte)(unsafe.Pointer(&fp)), 8)
	mtx := &f.stripes[fp%fingerprintStripes]
	mtx.Lock()
	defer mtx.Unlock()
	var buf [16]byte
	if v, ok := f.seen.HasGet(buf[:0], k); ok && len(v) == 16 {
		first, last := int64(binary.LittleEndian.Uint64(v)), int64(binary.LittleEndian.Uint64(v[8:]))
		if minMs >= first && maxMs <= last+seriesIndexLagMs {
			return 0, 0, false
		}
		minMs, maxMs = min(minMs, first), max(maxMs, last)
	}
	binary.LittleEndian.PutUint64(buf[:8], uint64(minMs))
	binary.LittleEndian.PutUint64(buf[8:], uint64(maxMs))
	f.seen.Set(k, buf[:])
	return minMs, maxMs, true
}

func (f *Fingerprints) Reset() {
	f.seen.Reset()
}
