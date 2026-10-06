package metriccache

import (
	"sync"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/shared/metricindex"
	"github.com/metrico/qryn/v5/writer/utils/metadata"
)

const lagMs = int64(metricindex.SeriesIndexLag / time.Millisecond)

func emits(s *Fingerprints, fp uint64, minMs, maxMs int64) bool {
	_, _, ok := s.Emit(fp, minMs, maxMs)
	return ok
}

func TestFingerprintsFirstSightAndReset(t *testing.T) {
	s := NewFingerprints()
	if !emits(s, 42, 1000, 2000) {
		t.Fatal("first sight of a series must emit its row")
	}
	if emits(s, 42, 1500, 2000) {
		t.Fatal("samples inside the emitted bounds must not emit the row again")
	}
	if !emits(s, 43, 1000, 2000) {
		t.Fatal("first sight of another series must emit its row")
	}
	s.Reset()
	if !emits(s, 42, 1500, 2000) {
		t.Fatal("a series forgotten by the reset must emit its row again")
	}
}

func TestFingerprintsEmitAboveTheLag(t *testing.T) {
	s := NewFingerprints()
	s.Emit(1, 0, 1000)
	if emits(s, 1, 1000, 1000+lagMs) {
		t.Fatal("a sample within the lag of the emitted last_seen must not emit the row")
	}
	if !emits(s, 1, 1000, 1001+lagMs) {
		t.Fatal("a sample more than the lag above the emitted last_seen must emit the row")
	}
	if emits(s, 1, 1000, 1001+2*lagMs) {
		t.Fatal("the emitted last_seen must advance to the re-emitted row's")
	}
}

func TestFingerprintsEmitBelowFirstSeen(t *testing.T) {
	s := NewFingerprints()
	s.Emit(1, 10*lagMs, 10*lagMs)
	if !emits(s, 1, 10*lagMs-1, 10*lagMs) {
		t.Fatal("a sample below the emitted first_seen must emit the row")
	}
	if emits(s, 1, 10*lagMs-1, 10*lagMs) {
		t.Fatal("the emitted first_seen must move down to the re-emitted row's")
	}
	if emits(s, 1, 10*lagMs-1, 11*lagMs) {
		t.Fatal("re-emitting below must keep the emitted last_seen")
	}
}

func TestFingerprintsRowSpansEveryEmittedBound(t *testing.T) {
	s := NewFingerprints()
	s.Emit(1, 100*lagMs, 100*lagMs)
	first, last, ok := s.Emit(1, 10*lagMs, 11*lagMs)
	if !ok || first != 10*lagMs || last != 100*lagMs {
		t.Fatalf("an older batch must emit a row spanning both, got [%d, %d] %v, want [%d, %d] true",
			first, last, ok, 10*lagMs, 100*lagMs)
	}
	if emits(s, 1, 50*lagMs, 50*lagMs) {
		t.Fatal("a sample between the two batches lies inside the emitted row and must not emit")
	}
}

func TestFingerprintsEmitConcurrently(t *testing.T) {
	s := NewFingerprints()
	const n = 64
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fp := range uint64(8) {
				ts := int64(i) * lagMs
				s.Emit(fp, ts, ts)
			}
		}()
	}
	wg.Wait()
	for fp := range uint64(8) {
		if emits(s, fp, 0, (n-1)*lagMs) {
			t.Fatalf("series %d: the emitted bounds must cover every sample emitted concurrently", fp)
		}
	}
}

func TestMetadataChanged(t *testing.T) {
	s := NewMetadata()
	m := metadata.Entry{Type: "counter", Help: "Requests.", Unit: ""}
	if !s.Changed("http_requests_total", m) {
		t.Fatal("first metadata of a family must emit its row")
	}
	if s.Changed("http_requests_total", m) {
		t.Fatal("unchanged metadata must not emit a row")
	}
	m.Help = "All requests."
	if !s.Changed("http_requests_total", m) {
		t.Fatal("changed metadata must emit a row")
	}
	s.Reset()
	if !s.Changed("http_requests_total", m) {
		t.Fatal("metadata must be emitted again after the reset")
	}
}

func TestCachesPerNode(t *testing.T) {
	c := New()
	defer c.Stop()
	a := c.Node("a")
	if a != c.Node("a") {
		t.Fatal("the same node must return the same caches")
	}
	b := c.Node("b")
	a.Fingerprints.Emit(1, 1000, 1000)
	if !emits(b.Fingerprints, 1, 1000, 1000) {
		t.Fatal("nodes must not share the series cache")
	}
	a.Predecessors.Next(1, 1000, 1)
	if got := b.Predecessors.Next(1, 2000, 2); got != (Prev{0, 0, 1}) {
		t.Fatalf("nodes must not share predecessors, got %+v", got)
	}
}
