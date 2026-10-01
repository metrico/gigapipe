package metriccache

import (
	"testing"

	"github.com/metrico/qryn/v5/writer/utils/metadata"
)

func TestFingerprintsFirstSightAndReset(t *testing.T) {
	s := NewFingerprints()
	if !s.FirstSight(42) {
		t.Fatal("first sight of a series must emit its row")
	}
	if s.FirstSight(42) {
		t.Fatal("a known series must not emit its row again")
	}
	if !s.FirstSight(43) {
		t.Fatal("first sight of another series must emit its row")
	}
	s.Reset()
	if !s.FirstSight(42) {
		t.Fatal("a series forgotten by the reset must emit its row again")
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
	a.Fingerprints.FirstSight(1)
	if !b.Fingerprints.FirstSight(1) {
		t.Fatal("nodes must not share the series cache")
	}
	a.Predecessors.Next(1, 1000, 1)
	if got := b.Predecessors.Next(1, 2000, 2); got != (Prev{0, 0, 1}) {
		t.Fatalf("nodes must not share predecessors, got %+v", got)
	}
}
