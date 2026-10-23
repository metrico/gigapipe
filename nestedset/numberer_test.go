package nestedset

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSupportedVersion(t *testing.T) {
	for v, want := range map[string]bool{
		"24.9.3.128": true, "24.10.1": true, "25.3.14.14": true, "100.1": true,
		"24.8.14.39": false, "23.12.1": false, "24": false, "": false, "x.y": false,
	} {
		if got := SupportedVersion(v); got != want {
			t.Errorf("SupportedVersion(%q) = %v, want %v", v, got, want)
		}
	}
}

type logs struct {
	mu            sync.Mutex
	errors, infos []string
}

func (l *logs) Error(args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errors = append(l.errors, fmt.Sprint(args...))
}
func (l *logs) Info(args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.infos = append(l.infos, fmt.Sprint(args...))
}
func (l *logs) Debug(args ...any) {}

func (l *logs) count(substr string) (errors, infos int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.errors {
		if strings.Contains(e, substr) {
			errors++
		}
	}
	for _, i := range l.infos {
		if strings.Contains(i, substr) {
			infos++
		}
	}
	return errors, infos
}

var (
	testCfg = Config{Idle: time.Second, Interval: 2 * time.Millisecond, MaxSpans: 1}
	// slowCfg gives a leader's renewal room to land before its lease ends.
	slowCfg = Config{Idle: time.Second, Interval: 20 * time.Millisecond, MaxSpans: 1}
)

func TestStartRefusesAServerBelow24_9(t *testing.T) {
	l := &logs{}
	n := Start(context.Background(), &fakeStore{version: "24.8.1.1", lease: &fakeLease{}}, testCfg, l)
	if n != nil || len(l.errors) != 1 || !strings.Contains(l.errors[0], "24.9") {
		t.Errorf("numberer %v, errors %q; want none started and one error naming 24.9", n, l.errors)
	}
}

func TestStartRunsPassesUntilStopped(t *testing.T) {
	l := &logs{}
	srv := newFakeStore(&fakeLease{})
	n := Start(context.Background(), srv, testCfg, l)
	if n == nil {
		t.Fatalf("not started: %q", l.errors)
	}
	srv.waitPasses(t, 3)
	n.Stop()
	before := srv.passCount()
	time.Sleep(20 * time.Millisecond)
	if srv.passCount() != before {
		t.Error("a pass ran after Stop")
	}
	if _, won := l.count("won term"); won != 1 {
		t.Errorf("infos %q, want one win", l.infos)
	}
}

func TestOnlyTheLeaderRunsPassesAndAnotherTakesOverWhenItStops(t *testing.T) {
	shared := &fakeLease{}
	a, b := newFakeStore(shared), newFakeStore(shared)
	la, lb := &logs{}, &logs{}
	na := Start(context.Background(), a, slowCfg, la)
	a.waitPasses(t, 1)
	nb := Start(context.Background(), b, slowCfg, lb)
	defer nb.Stop()
	time.Sleep(3 * slowCfg.LeaseLen())
	if b.passCount() != 0 {
		t.Fatalf("the follower ran %d passes while the leader lived", b.passCount())
	}
	na.Stop()
	b.waitPasses(t, 1)
	if _, won := lb.count("won term"); won != 1 {
		t.Errorf("follower infos %q, want one win", lb.infos)
	}
}

func TestALeaderWhosePassesFailHandsOver(t *testing.T) {
	shared := &fakeLease{}
	a, b := newFakeStore(shared), newFakeStore(shared)
	a.failPasses.Store(true)
	la, lb := &logs{}, &logs{}
	na := Start(context.Background(), a, testCfg, la)
	defer na.Stop()
	a.waitPasses(t, 1)
	nb := Start(context.Background(), b, testCfg, lb)
	defer nb.Stop()
	b.waitPasses(t, 1)
	waitFor(t, func() bool { _, down := la.count("stepped down to term"); return down == 1 })
}

func TestALeaseOutageIsLoggedOnceAndPausesPasses(t *testing.T) {
	l := &logs{}
	srv := newFakeStore(&fakeLease{frozen: t0})
	srv.lease.fail.Store(true)
	n := Start(context.Background(), srv, testCfg, l)
	defer n.Stop()
	time.Sleep(10 * testCfg.Interval)
	if srv.passCount() != 0 {
		t.Errorf("%d passes during the outage", srv.passCount())
	}
	srv.lease.fail.Store(false)
	srv.waitPasses(t, 1)
	srv.lease.fail.Store(true)
	time.Sleep(10 * testCfg.Interval)
	srv.lease.fail.Store(false)
	srv.waitPasses(t, srv.passCount()+1)
	if errs, _ := l.count(leaseTableName); errs != 2 {
		t.Errorf("errors %q, want one naming %s per outage", l.errors, leaseTableName)
	}
	if _, over := l.count("lease outage over"); over != 2 {
		t.Errorf("infos %q, want one end per outage", l.infos)
	}
	if _, won := l.count("won term"); won != 1 {
		t.Errorf("infos %q, want the leader to resume its term silently", l.infos)
	}
}

func TestAStaleClaimStoredLateLosesItsTerm(t *testing.T) {
	f := &fakeLease{}
	srv := newFakeStore(f)
	me := &elector{store: srv, me: "me", leaseLen: time.Minute, log: &logs{}, passOK: true}
	if !me.step(context.Background()) {
		t.Fatal("the only claimant does not lead")
	}
	rows, _, _ := f.read()
	term := rows[0].Term
	time.Sleep(2 * time.Millisecond)
	f.forgetTokens()
	if err := f.write(LeaseWrite{Term: term, Holder: "late", Claim: true, LeaseLen: time.Minute, Token: claimToken(term)}); err != nil {
		t.Fatal(err)
	}
	late := &elector{store: srv, me: "late", leaseLen: time.Minute, log: &logs{}, passOK: true}
	if !me.step(context.Background()) || late.step(context.Background()) {
		t.Error("the claim stored late took the term")
	}
}

// fakeLease is an in-memory lease table that drops a write whose token it
// has seen. Its server time is the local clock, or frozen when set.
type fakeLease struct {
	frozen time.Time
	mu     sync.Mutex
	rows   []LeaseRow
	tokens map[string]bool
	fail   atomic.Bool
}

func (f *fakeLease) now() time.Time {
	if !f.frozen.IsZero() {
		return f.frozen
	}
	return time.Now()
}

func (f *fakeLease) read() ([]LeaseRow, time.Time, error) {
	if f.fail.Load() {
		return nil, time.Time{}, errors.New("reading " + leaseTableName + ": connection refused")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.rows), f.now(), nil
}

// write stores w, expiring w.LeaseLen after the fake server's time.
func (f *fakeLease) write(w LeaseWrite) error {
	if f.fail.Load() {
		return errors.New("writing " + leaseTableName + ": connection refused")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.tokens == nil {
		f.tokens = map[string]bool{}
	}
	if !f.tokens[w.Token] {
		f.tokens[w.Token] = true
		f.rows = append(f.rows, LeaseRow{w.Term, w.Holder, w.Claim, f.now().Add(w.LeaseLen)})
	}
	return nil
}

// forgetTokens empties the deduplication window.
func (f *fakeLease) forgetTokens() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tokens = nil
}

type fakeStore struct {
	version    string
	lease      *fakeLease
	passes     atomic.Int64
	failPasses atomic.Bool
}

func newFakeStore(l *fakeLease) *fakeStore { return &fakeStore{version: "24.9.3.128", lease: l} }

func (f *fakeStore) Version(context.Context) (string, error) { return f.version, nil }

func (f *fakeStore) Pass(context.Context, time.Duration, int) error {
	f.passes.Add(1)
	if f.failPasses.Load() {
		return errors.New("pass failed")
	}
	return nil
}

func (f *fakeStore) ReadLease(context.Context) ([]LeaseRow, time.Time, error) { return f.lease.read() }

func (f *fakeStore) WriteLease(_ context.Context, w LeaseWrite) error {
	return f.lease.write(w)
}

func (f *fakeStore) passCount() int64 { return f.passes.Load() }

func (f *fakeStore) waitPasses(t *testing.T, n int64) {
	t.Helper()
	waitFor(t, func() bool { return f.passCount() >= n })
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(time.Millisecond)
	}
}
