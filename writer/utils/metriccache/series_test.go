package metriccache

import (
	"testing"

	"github.com/metrico/qryn/v5/writer/utils/metadata"
)

func TestSeriesFirstSightAndReset(t *testing.T) {
	s := NewSeries()
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

func TestSeriesMetadataChanged(t *testing.T) {
	s := NewSeries()
	m := metadata.Entry{Type: "counter", Help: "Requests.", Unit: ""}
	if !s.MetadataChanged("http_requests_total", m) {
		t.Fatal("first metadata of a family must emit its row")
	}
	if s.MetadataChanged("http_requests_total", m) {
		t.Fatal("unchanged metadata must not emit a row")
	}
	m.Help = "All requests."
	if !s.MetadataChanged("http_requests_total", m) {
		t.Fatal("changed metadata must emit a row")
	}
	s.Reset()
	if !s.MetadataChanged("http_requests_total", m) {
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
	a.Series.FirstSight(1)
	if !b.Series.FirstSight(1) {
		t.Fatal("nodes must not share the series cache")
	}
	a.Predecessors.Next(1, 1000, 1)
	if got := b.Predecessors.Next(1, 2000, 2); got != (Prev{0, 0, 1}) {
		t.Fatalf("nodes must not share predecessors, got %+v", got)
	}
}
