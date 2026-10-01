package ruler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
)

type clockWaiter struct {
	at time.Time
	ch chan time.Time
}

// fakeClock fires After channels only when Advance moves past their
// deadline, and reports each new deadline on waiting.
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []clockWaiter
	waiting chan time.Time
}

func newFakeClock(now time.Time) *fakeClock {
	return &fakeClock{now: now, waiting: make(chan time.Time, 16)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- c.now
		return ch
	}
	at := c.now.Add(d)
	c.waiters = append(c.waiters, clockWaiter{at, ch})
	c.waiting <- at
	return ch
}

func (c *fakeClock) Advance(to time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = to
	kept := c.waiters[:0]
	for _, w := range c.waiters {
		if w.at.After(to) {
			kept = append(kept, w)
			continue
		}
		w.ch <- to
	}
	c.waiters = kept
}

func receive[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

func TestSchedule_TicksLandOnTheIntervalGrid(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := newFakeClock(base.Add(7300 * time.Millisecond))
	eval := &fakeEvaluator{vec: sampleVec()}
	writer := &fakeWriter{written: make(chan promql.Vector, 16)}
	reader := &fakeReader{groups: NamespaceRuleGroups{
		"ns": {{Name: "g", Interval: "15s", Rules: []Rule{{Record: "rec", Expr: "up"}}}},
	}}
	m := NewRuleManager(eval, reader, writer, time.Hour)
	m.clock = clock
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	if at := receive(t, clock.waiting, "first wait"); !at.Equal(base.Add(15 * time.Second)) {
		t.Fatalf("first tick waits until %s, want the next grid point %s", at, base.Add(15*time.Second))
	}

	steps := []struct{ advanceTo, tick, nextWait time.Time }{
		{base.Add(15*time.Second + 4*time.Millisecond), base.Add(15 * time.Second), base.Add(30 * time.Second)},
		{base.Add(30 * time.Second), base.Add(30 * time.Second), base.Add(45 * time.Second)},
		{base.Add(62 * time.Second), base.Add(60 * time.Second), base.Add(75 * time.Second)},
	}
	for i, st := range steps {
		clock.Advance(st.advanceTo)
		v := receive(t, writer.written, "write-back")
		if len(v) != 1 || v[0].T != st.tick.UnixMilli() {
			t.Errorf("tick %d: written %v, want one sample at %d", i, v, st.tick.UnixMilli())
		}
		eval.mu.Lock()
		evalAt := eval.times[len(eval.times)-1]
		eval.mu.Unlock()
		if !evalAt.Equal(st.tick) || evalAt.Location() != time.UTC {
			t.Errorf("tick %d: evaluated at %s, want %s UTC", i, evalAt, st.tick)
		}
		if at := receive(t, clock.waiting, "next wait"); !at.Equal(st.nextWait) {
			t.Errorf("tick %d: next wait until %s, want %s", i, at, st.nextWait)
		}
	}
}

func TestSchedule_SkipsIntervalsOffTheMillisecondGrid(t *testing.T) {
	m := NewRuleManager(&fakeEvaluator{}, &fakeReader{}, &fakeWriter{}, time.Hour)
	m.ctx = context.Background()

	m.updateRoutines(NamespaceRuleGroups{"ns": {
		{Name: "frac", Interval: "1500us", Rules: []Rule{{Record: "a", Expr: "up"}}},
		{Name: "sub", Interval: "500us", Rules: []Rule{{Record: "b", Expr: "up"}}},
	}})

	if n := len(m.routines); n != 0 {
		t.Errorf("started %d routines, want none", n)
	}
}
