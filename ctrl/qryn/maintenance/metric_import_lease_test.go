package maintenance

import (
	"context"
	"maps"
	"testing"
	"time"
)

// memRecords holds the import's settings records in memory, stamped by the server clock.
type memRecords struct {
	values map[string]string
	at     map[string]time.Time
	server *testClock
}

func newMemRecords(server *testClock) *memRecords {
	return &memRecords{values: map[string]string{}, at: map[string]time.Time{}, server: server}
}

func (m *memRecords) all(context.Context) (map[string]string, error) {
	return maps.Clone(m.values), nil
}

func (m *memRecords) put(_ context.Context, name, value string) error {
	m.values[name] = value
	m.at[name] = m.server.t
	return nil
}

func (m *memRecords) age(_ context.Context, name string) (time.Duration, error) {
	return m.server.t.Sub(m.at[name]), nil
}

type testClock struct{ t time.Time }

func (c *testClock) now() time.Time { return c.t }

func newTestLease(records *memRecords, instance string, clock *testClock) *importLease {
	return &importLease{
		records:  records,
		instance: instance,
		ttl:      3 * time.Minute,
		now:      clock.now,
		sleep:    func(context.Context, time.Duration) error { return nil },
	}
}

func holds(t *testing.T, l *importLease, op func(context.Context) (bool, error)) bool {
	t.Helper()
	ok, err := op(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

func TestAFreeLeaseIsAcquired(t *testing.T) {
	clock := &testClock{at("2026-10-02 09:00")}
	records := newMemRecords(clock)
	a := newTestLease(records, "a", clock)
	if !holds(t, a, a.acquire) {
		t.Fatal("free lease not acquired")
	}
	if records.values["lease"] != "a:1790931600" {
		t.Errorf("lease = %q", records.values["lease"])
	}
}

func TestAHeldLeaseIsNotTakenUntilItExpires(t *testing.T) {
	clock := &testClock{at("2026-10-02 09:00")}
	records := newMemRecords(clock)
	a, b := newTestLease(records, "a", clock), newTestLease(records, "b", clock)
	holds(t, a, a.acquire)
	clock.t = clock.t.Add(2 * time.Minute)
	if holds(t, b, b.acquire) {
		t.Fatal("b took a fresh lease")
	}
	if !holds(t, a, a.renew) {
		t.Fatal("a lost its own lease")
	}
	clock.t = clock.t.Add(2 * time.Minute)
	if holds(t, b, b.acquire) {
		t.Fatal("b took a renewed lease")
	}
	clock.t = clock.t.Add(2 * time.Minute)
	if !holds(t, b, b.acquire) {
		t.Fatal("b did not take an expired lease")
	}
	if holds(t, a, a.renew) {
		t.Fatal("a renewed a lease b took over")
	}
}

func TestAReleasedLeaseIsFreeAtOnce(t *testing.T) {
	clock := &testClock{at("2026-10-02 09:00")}
	records := newMemRecords(clock)
	a, b := newTestLease(records, "a", clock), newTestLease(records, "b", clock)
	holds(t, a, a.acquire)
	if err := a.release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !holds(t, b, b.acquire) {
		t.Fatal("b did not take a released lease")
	}
	if err := a.release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if records.values["lease"] == "" {
		t.Fatal("a released b's lease")
	}
}

func TestAnAcquireOverwrittenWhileSettlingIsLost(t *testing.T) {
	clock := &testClock{at("2026-10-02 09:00")}
	records := newMemRecords(clock)
	a := newTestLease(records, "a", clock)
	a.sleep = func(context.Context, time.Duration) error {
		records.values["lease"] = "b:1790931600"
		return nil
	}
	if holds(t, a, a.acquire) {
		t.Fatal("a holds a lease b wrote after it")
	}
}

func TestALeaseAgesByTheServerClock(t *testing.T) {
	server := &testClock{at("2026-10-02 09:00")}
	records := newMemRecords(server)
	a := newTestLease(records, "a", server)
	holds(t, a, a.acquire)
	b := newTestLease(records, "b", &testClock{server.t.Add(10 * time.Minute)})
	if holds(t, b, b.acquire) {
		t.Fatal("b took a fresh lease by its own clock")
	}
}

func TestALeaseHolderIsReadBack(t *testing.T) {
	clock := &testClock{at("2026-10-02 09:00")}
	records := newMemRecords(clock)
	a, b := newTestLease(records, "a", clock), newTestLease(records, "b", clock)
	holds(t, a, a.acquire)
	if !holds(t, a, a.held) || holds(t, b, b.held) {
		t.Fatal("held does not name a as the holder")
	}
}
