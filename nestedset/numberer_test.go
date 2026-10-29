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

// read returns every row, each with the cursor of the latest row by term and
// expiry that has one.
func (f *fakeLease) read() ([]LeaseRow, time.Time, error) {
	if f.fail.Load() {
		return nil, time.Time{}, errors.New("reading " + leaseTableName + ": connection refused")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var latest *LeaseRow
	for i, r := range f.rows {
		if cursorSet(r.Cursor) && (latest == nil || r.Term > latest.Term || r.Term == latest.Term && r.ExpiresAt.After(latest.ExpiresAt)) {
			latest = &f.rows[i]
		}
	}
	rows := slices.Clone(f.rows)
	for i := range rows {
		rows[i].Cursor = time.Time{}
		if latest != nil {
			rows[i].Cursor = latest.Cursor
		}
	}
	return rows, f.now(), nil
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
		f.rows = append(f.rows, LeaseRow{Term: w.Term, Holder: w.Holder, Claim: w.Claim, ExpiresAt: f.now().Add(w.LeaseLen), Cursor: w.Cursor})
	}
	return nil
}

// forgetTokens empties the deduplication window.
func (f *fakeLease) forgetTokens() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tokens = nil
}

// fakeStore passes advance the cursor by a minute and return readAt of their
// index; a failed one keeps the cursor and returns failRead.
type fakeStore struct {
	version    string
	lease      *fakeLease
	passes     atomic.Int64
	failPasses atomic.Bool
	failRead   RunningRead
	mu         sync.Mutex
	cursors    []time.Time     // the cursor each pass started from
	reads      [][]RunningRead // the reads each pass started from
}

func newFakeStore(l *fakeLease) *fakeStore { return &fakeStore{version: "24.9.3.128", lease: l} }

func (f *fakeStore) Version(context.Context) (string, error) { return f.version, nil }

func (f *fakeStore) Pass(_ context.Context, _ time.Duration, _ int, cursor time.Time, reads Reads) (time.Time, RunningRead, error) {
	f.mu.Lock()
	f.cursors = append(f.cursors, cursor)
	f.reads = append(f.reads, reads.all())
	f.mu.Unlock()
	i := f.passes.Add(1) - 1
	if f.failPasses.Load() {
		return cursor, f.failRead, errors.New("pass failed")
	}
	return cursor.Add(time.Minute), readAt(int(i)), nil
}

func (f *fakeStore) startedFrom() []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.cursors)
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

func TestEachPassStartsFromTheCursorThePreviousOneReturned(t *testing.T) {
	srv := newFakeStore(&fakeLease{})
	n := Start(context.Background(), srv, testCfg, &logs{})
	srv.waitPasses(t, 3)
	n.Stop()
	got := srv.startedFrom()
	for i, c := range got {
		if want := (time.Time{}).Add(time.Duration(i) * time.Minute); !c.Equal(want) {
			t.Fatalf("passes started from %v, want each a minute after the previous, from zero", got)
		}
	}
}

// readAt is the fake store's read of its pass m.
func readAt(m int) RunningRead {
	c := t0.Add(time.Duration(m) * time.Minute)
	return RunningRead{At: c.Add(time.Second), Start: c}
}

func TestEachPassStartsFromTheReadsTheEarlierOnesReturned(t *testing.T) {
	srv := newFakeStore(&fakeLease{})
	n := Start(context.Background(), srv, testCfg, &logs{})
	srv.waitPasses(t, 10)
	n.Stop()
	srv.mu.Lock()
	got := slices.Clone(srv.reads)
	srv.mu.Unlock()
	var want Reads
	for i, reads := range got {
		if !slices.Equal(reads, want.all()) {
			t.Fatalf("pass %d started from reads %v, want %v", i, reads, want.all())
		}
		want = want.add(readAt(i))
	}
}

func TestAFailedPassAddsItsReadOnlyWhenItTookOne(t *testing.T) {
	srv := newFakeStore(&fakeLease{})
	e := &elector{store: srv, me: "me", leaseLen: time.Minute, log: &logs{}, passOK: true}
	if !e.step(context.Background()) {
		t.Fatal("the only claimant does not lead")
	}
	if err := e.pass(context.Background(), testCfg); err != nil {
		t.Fatal(err)
	}
	srv.failPasses.Store(true)
	if err := e.pass(context.Background(), testCfg); err == nil {
		t.Fatal("want the pass error")
	}
	if want := []RunningRead{readAt(0)}; !slices.Equal(e.reads.all(), want) {
		t.Errorf("reads %v after a pass that took no read, want %v", e.reads, want)
	}
	srv.failRead = readAt(5)
	if err := e.pass(context.Background(), testCfg); err == nil {
		t.Fatal("want the pass error")
	}
	if want := []RunningRead{readAt(0), readAt(5)}; !slices.Equal(e.reads.all(), want) {
		t.Errorf("reads %v after a pass that failed after its read, want %v", e.reads, want)
	}
}

func TestAnInstanceThatLeadsAgainStartsFromItsEarlierReads(t *testing.T) {
	f := &fakeLease{frozen: at(1000)}
	srv := newFakeStore(f)
	e := &elector{store: srv, me: "a", leaseLen: time.Minute, log: &logs{}, passOK: true}
	if !e.step(context.Background()) {
		t.Fatal("the only claimant does not lead")
	}
	if err := e.pass(context.Background(), testCfg); err != nil {
		t.Fatal(err)
	}
	f.frozen = at(2000)
	f.rows = append(f.rows, LeaseRow{Term: 1_000_000_000_000, Holder: "b", Claim: true, ExpiresAt: at(2000).Add(time.Minute)})
	if e.step(context.Background()) {
		t.Fatal("a led while b held a later term")
	}
	f.frozen = at(2000).Add(2 * time.Minute)
	if !e.step(context.Background()) {
		t.Fatal("a did not claim b's expired lease")
	}
	if err := e.pass(context.Background(), testCfg); err != nil {
		t.Fatal(err)
	}
	if got := srv.reads[1]; !slices.Equal(got, []RunningRead{readAt(0)}) {
		t.Errorf("the pass of a's new term started from reads %v, want its earlier term's %v", got, readAt(0))
	}
}

func TestARenewalCarriesTheLastPassesCursor(t *testing.T) {
	f := &fakeLease{}
	srv := newFakeStore(f)
	e := &elector{store: srv, me: "me", leaseLen: time.Minute, log: &logs{}, passOK: true}
	if !e.step(context.Background()) {
		t.Fatal("the only claimant does not lead")
	}
	if err := e.pass(context.Background(), testCfg); err != nil {
		t.Fatal(err)
	}
	if !e.step(context.Background()) {
		t.Fatal("the holder does not renew")
	}
	if r := f.rows[len(f.rows)-1]; r.Claim || !r.Cursor.Equal(time.Time{}.Add(time.Minute)) {
		t.Errorf("last lease row %+v, want a renewal carrying the pass's cursor", r)
	}
}

func TestAClaimCarriesTheLatestCursorByTermAndExpiry(t *testing.T) {
	c := func(m int) time.Time { return t0.Add(time.Duration(m) * time.Minute) }
	f := &fakeLease{frozen: at(1000), rows: []LeaseRow{
		{Term: 4, Holder: "x", Claim: true, ExpiresAt: at(100), Cursor: c(9)},
		{Term: 5, Holder: "old", Claim: true, ExpiresAt: at(200), Cursor: c(1)},
		{Term: 5, Holder: "old", ExpiresAt: at(250), Cursor: c(8)},
		{Term: 5, Holder: "old", ExpiresAt: at(300), Cursor: c(2)},
		{Term: 5, Holder: "old", ExpiresAt: at(310)},
	}}
	srv := newFakeStore(f)
	e := &elector{store: srv, me: "new", leaseLen: time.Minute, log: &logs{}, passOK: true}
	if !e.step(context.Background()) {
		t.Fatal("the claimant of the expired lease does not lead")
	}
	if r := f.rows[len(f.rows)-1]; r.Term != 6 || !r.Claim || !r.Cursor.Equal(c(2)) {
		t.Errorf("claim %+v, want term 6 carrying the cursor of term 5's latest renewal %v", r, c(2))
	}
	if err := e.pass(context.Background(), testCfg); err != nil {
		t.Fatal(err)
	}
	if got := srv.startedFrom(); len(got) != 1 || !got[0].Equal(c(2)) {
		t.Errorf("the winner's pass started from %v, want %v", got, c(2))
	}
}

func TestAFailedPassKeepsTheElectorsCursor(t *testing.T) {
	srv := newFakeStore(&fakeLease{})
	e := &elector{store: srv, me: "me", leaseLen: time.Minute, log: &logs{}, passOK: true}
	if !e.step(context.Background()) {
		t.Fatal("the only claimant does not lead")
	}
	if err := e.pass(context.Background(), testCfg); err != nil {
		t.Fatal(err)
	}
	srv.failPasses.Store(true)
	if err := e.pass(context.Background(), testCfg); err == nil {
		t.Fatal("want the pass error")
	}
	srv.failPasses.Store(false)
	if err := e.pass(context.Background(), testCfg); err != nil {
		t.Fatal(err)
	}
	want := time.Time{}.Add(time.Minute)
	if got := srv.startedFrom(); len(got) != 3 || !got[1].Equal(want) || !got[2].Equal(want) {
		t.Errorf("passes started from %v, want the retry from the failed pass's cursor %v", got, want)
	}
}
