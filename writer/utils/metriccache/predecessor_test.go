package metriccache

import (
	"math"
	"sort"
	"sync"
	"testing"
	"time"
)

var staleNaN = math.Float64frombits(0x7ff0000000000002)

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newTestPredecessors() (*Predecessors, *clock) {
	c := &clock{t: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)}
	return NewPredecessors(time.Hour, c.now), c
}

func TestPredecessorsInOrder(t *testing.T) {
	p, _ := newTestPredecessors()
	if got, want := p.Next(1, 1000, 5), (Prev{0, 0, 1}); got != want {
		t.Fatalf("first sample: got %+v, want %+v", got, want)
	}
	if got, want := p.Next(1, 2000, 7), (Prev{1000, 5, 1}); got != want {
		t.Fatalf("second sample: got %+v, want %+v", got, want)
	}
	if got, want := p.Next(2, 2000, 1), (Prev{0, 0, 1}); got != want {
		t.Fatalf("other series: got %+v, want %+v", got, want)
	}
}

func TestPredecessorsOutOfOrderAndDuplicate(t *testing.T) {
	p, _ := newTestPredecessors()
	p.Next(1, 1000, 5)
	p.Next(1, 3000, 9)
	if got, want := p.Next(1, 2000, 7), (Prev{3000, 9, 0}); got != want {
		t.Fatalf("out of order: got %+v, want %+v", got, want)
	}
	if got, want := p.Next(1, 3000, 10), (Prev{3000, 9, 0}); got != want {
		t.Fatalf("duplicate: got %+v, want %+v", got, want)
	}
	if got, want := p.Next(1, 4000, 11), (Prev{3000, 9, 1}); got != want {
		t.Fatalf("after out of order and duplicate: got %+v, want %+v", got, want)
	}
}

func TestPredecessorsStaleMarker(t *testing.T) {
	p, _ := newTestPredecessors()
	if got, want := p.Next(1, 500, staleNaN), (Prev{0, 0, 1}); got != want {
		t.Fatalf("marker as first sample: got %+v, want %+v", got, want)
	}
	if got, want := p.Next(1, 1000, 5), (Prev{0, 0, 1}); got != want {
		t.Fatalf("sample after a first marker: got %+v, want %+v", got, want)
	}
	if got, want := p.Next(1, 2000, staleNaN), (Prev{1000, 5, 1}); got != want {
		t.Fatalf("marker: got %+v, want %+v", got, want)
	}
	if got, want := p.Next(1, 3000, 6), (Prev{1000, 5, 1}); got != want {
		t.Fatalf("sample after marker: got %+v, want %+v", got, want)
	}
}

func TestPredecessorsNaNIsAPredecessor(t *testing.T) {
	p, _ := newTestPredecessors()
	p.Next(1, 1000, math.NaN())
	got := p.Next(1, 2000, 1)
	if got.TimestampMs != 1000 || !math.IsNaN(got.Value) || got.Aggregate != 1 {
		t.Fatalf("got %+v, want the plain NaN at 1000 as predecessor", got)
	}
}

func TestPredecessorsEvictIdle(t *testing.T) {
	p, c := newTestPredecessors()
	p.Next(1, 1000, 5)
	p.Next(2, 1000, 5)
	c.t = c.t.Add(30 * time.Minute)
	p.Next(2, 2000, 6)
	c.t = c.t.Add(30 * time.Minute)
	p.EvictIdle()
	if got, want := p.Next(1, 3000, 7), (Prev{0, 0, 1}); got != want {
		t.Fatalf("evicted series: got %+v, want %+v", got, want)
	}
	if got, want := p.Next(2, 3000, 7), (Prev{2000, 6, 1}); got != want {
		t.Fatalf("live series: got %+v, want %+v", got, want)
	}
}

// Concurrent samples of one series: the accepted non-stale samples form one
// chain in timestamp order, each paired with the previous link.
func TestPredecessorsConcurrentChain(t *testing.T) {
	p, _ := newTestPredecessors()
	const workers, perWorker = 8, 2000
	type row struct {
		ts   int64
		v    float64
		prev Prev
	}
	results := make([][]row, workers)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := range perWorker {
				ts := int64(k*workers+w) + 1
				v := float64(ts)
				if ts%7 == 0 {
					v = staleNaN
				}
				results[w] = append(results[w], row{ts, v, p.Next(1, ts, v)})
			}
		}()
	}
	wg.Wait()

	var chain []row
	for _, rs := range results {
		for _, r := range rs {
			if r.prev.Aggregate == 1 && !math.IsNaN(r.v) {
				chain = append(chain, r)
			}
		}
	}
	sort.Slice(chain, func(i, j int) bool { return chain[i].ts < chain[j].ts })
	if len(chain) == 0 {
		t.Fatal("no sample was aggregated")
	}
	if chain[0].prev.TimestampMs != 0 {
		t.Fatalf("first link: got predecessor %+v, want none", chain[0].prev)
	}
	for i := 1; i < len(chain); i++ {
		if chain[i].prev.TimestampMs != chain[i-1].ts || chain[i].prev.Value != chain[i-1].v {
			t.Fatalf("link %d at %d: got predecessor %+v, want (%d, %v)",
				i, chain[i].ts, chain[i].prev, chain[i-1].ts, chain[i-1].v)
		}
	}
}
