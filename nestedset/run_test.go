package nestedset

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

// recorder is an inserter and a reader that records each statement. The
// running read reports running (running0 when unset), taken at at (a second
// after it when unset), the frontier read a batch of n traces and frontier; fail fails the
// statements with that prefix. reads are the reads the passes returned.
type recorder struct {
	running  time.Time
	at       time.Time
	n        uint64
	frontier time.Time
	fail     string
	got      []string
	reads    Reads
	last     RunningRead // the read the last pass returned
}

func (r *recorder) insert(_ context.Context, q string) error {
	r.got = append(r.got, q)
	if r.fail != "" && strings.HasPrefix(q, r.fail) {
		return errors.New("boom")
	}
	return nil
}

func (r *recorder) read(_ context.Context, q string, dest ...any) error {
	r.got = append(r.got, q)
	if r.fail != "" && strings.HasPrefix(q, r.fail) {
		return errors.New("boom")
	}
	if strings.HasPrefix(q, "running") {
		start := r.running
		if start.IsZero() {
			start = running0
		}
		at := r.at
		if at.IsZero() {
			at = start.Add(time.Second)
		}
		*dest[0].(*time.Time), *dest[1].(*time.Time) = start, at
		return nil
	}
	*dest[0].(*uint64), *dest[1].(*time.Time) = r.n, r.frontier
	return nil
}

func (r *recorder) run(cursor time.Time) (time.Time, error) {
	next, read, err := run(context.Background(), r.insert, r.read, fakeStatements, cursor, r.reads)
	r.last = read
	r.reads = r.reads.add(read)
	return next, err
}

var fakeStatements = statements{
	running: "running",
	batch: func(id string, cursor time.Time, running RunningRead, reads Reads) string {
		q := "batch " + id + " " + cursor.UTC().Format(time.RFC3339) + " " + running.Start.UTC().Format(time.RFC3339) + " reads"
		for _, r := range reads.all() {
			q += " " + r.At.UTC().Format(time.RFC3339) + " " + r.Start.UTC().Format(time.RFC3339)
		}
		return q
	},
	frontier: func(id string) string { return "frontier " + id },
	number:   func(id string) string { return "number " + id },
}

var (
	cursor0  = time.Date(2026, 10, 29, 10, 0, 0, 0, time.UTC)
	frontier = cursor0.Add(time.Minute)
	running0 = cursor0.Add(2 * time.Minute)
)

func TestAPassWithAnEmptyBatchSkipsTheNumberingAndAdvances(t *testing.T) {
	r := &recorder{n: 0, frontier: frontier}
	next, err := r.run(cursor0)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.got) != 3 || r.got[0] != "running" || !strings.HasPrefix(r.got[1], "batch ") || !strings.HasPrefix(r.got[2], "frontier ") {
		t.Errorf("ran %q, want only the running read, the batch and the frontier read", r.got)
	}
	if !next.Equal(frontier) {
		t.Errorf("next cursor %v, want the frontier %v", next, frontier)
	}
}

func TestAPassScansFromItsCursorAndAdvancesToTheFrontier(t *testing.T) {
	r := &recorder{running: running0, n: 3, frontier: frontier}
	next, err := r.run(cursor0)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.got) != 4 || !strings.Contains(r.got[1], " 2026-10-29T10:00:00Z 2026-10-29T10:02:00Z ") || !strings.HasPrefix(r.got[3], "number ") {
		t.Errorf("ran %q, want the batch from the cursor cut off at the running read, the frontier read and the numbering", r.got)
	}
	if !next.Equal(frontier) {
		t.Errorf("next cursor %v, want the frontier %v", next, frontier)
	}
}

func TestAFailedPassKeepsTheCursor(t *testing.T) {
	for _, step := range []string{"running", "batch", "frontier", "number"} {
		r := &recorder{n: 3, frontier: frontier, fail: step}
		next, err := r.run(cursor0)
		if err == nil || !next.Equal(cursor0) {
			t.Errorf("%s failed: cursor %v, err %v; want %v and the error", step, next, err, cursor0)
		}
	}
}

func TestAPassNumbersTheBatchItWrote(t *testing.T) {
	r := &recorder{n: 3, frontier: frontier}
	if _, err := r.run(cursor0); err != nil {
		t.Fatal(err)
	}
	var batch, read, number string
	if len(r.got) != 4 {
		t.Fatalf("ran %q, want the running read, the batch, the frontier read, then the numbering", r.got)
	}
	if n, err := fmt.Sscanf(r.got[1], "batch %s", &batch); n != 1 || err != nil {
		t.Fatalf("batch statement %q: %v", r.got[1], err)
	}
	if n, err := fmt.Sscanf(r.got[2], "frontier %s", &read); n != 1 || err != nil {
		t.Fatalf("frontier read %q: %v", r.got[2], err)
	}
	if n, err := fmt.Sscanf(r.got[3], "number %s", &number); n != 1 || err != nil {
		t.Fatalf("numbering statement %q: %v", r.got[3], err)
	}
	if number != batch || read != batch {
		t.Errorf("ran %q, want both statements with one pass id", r.got)
	}
}

func TestEachPassHasItsOwnID(t *testing.T) {
	r := &recorder{frontier: frontier}
	for range 2 {
		if _, err := r.run(cursor0); err != nil {
			t.Fatal(err)
		}
	}
	if len(r.got) != 6 || r.got[1] == r.got[4] {
		t.Errorf("ran %q, want two batches with different pass ids", r.got)
	}
}

func TestAFailedBatchSkipsTheNumbering(t *testing.T) {
	r := &recorder{n: 3, fail: "batch"}
	if _, err := r.run(cursor0); err == nil || len(r.got) != 2 {
		t.Errorf("err %v after %q, want the batch error and no numbering", err, r.got)
	}
}

func TestAPassWithoutAFrontierKeepsTheCursorAndSkipsTheNumbering(t *testing.T) {
	for _, f := range []time.Time{{}, time.UnixMilli(0)} {
		r := &recorder{n: 3, frontier: f}
		next, err := r.run(cursor0)
		if !errors.Is(err, errNoFrontier) || !next.Equal(cursor0) || len(r.got) != 3 {
			t.Errorf("frontier %v: cursor %v, err %v after %q; want %v, errNoFrontier and no numbering", f, next, err, r.got, cursor0)
		}
	}
}

func TestAPassWhoseRunningReadReachedNoNodeKeepsTheCursor(t *testing.T) {
	r := &recorder{running: time.UnixMilli(0), n: 3, frontier: frontier}
	next, err := r.run(cursor0)
	if !errors.Is(err, errNoRunning) || !next.Equal(cursor0) || len(r.got) != 1 {
		t.Errorf("cursor %v, err %v after %q; want %v, errNoRunning and no batch", next, err, r.got, cursor0)
	}
}

func TestTheRunningReadSkipsAReplicaThatDoesNotConnectWithinSeconds(t *testing.T) {
	q := clusterStatements("c", "db", time.Second, 10).running
	for _, want := range []string{
		"FROM clusterAllReplicas('c', view(", "skip_unavailable_shards = 1", "connections_with_failover_max_tries = 1",
		"connect_timeout_with_failover_ms = 1000", "handshake_timeout_ms = 2000", "send_timeout = 3", "receive_timeout = 3",
	} {
		if !strings.Contains(q, want) {
			t.Errorf("running read lacks %q:\n%s", want, q)
		}
	}
}

func TestTheReplicaDelayHoldsTheIdlePeriodWhileLogEntriesAreUnpulled(t *testing.T) {
	q := tables{db: "db"}.replicaDelay(1500 * time.Millisecond)
	for _, want := range []string{
		"if(log_pointer <= log_max_index, greatest(absolute_delay, 2), absolute_delay)",
		"database = 'db' AND table IN ('tempo_traces', 'tempo_traces_touched')",
	} {
		if !strings.Contains(q, want) {
			t.Errorf("replica delay lacks %q:\n%s", want, q)
		}
	}
}

func TestTheReplicaSinceReadsBothTablesUnappliedEntries(t *testing.T) {
	q := tables{db: "db"}.replicaSince()
	for _, want := range []string{
		"if(log_pointer <= log_max_index, if(toUnixTimestamp(last_queue_update) = 0, now() - uptime(), last_queue_update), now())",
		"if(inserts_in_queue > 0, inserts_oldest_time, now())", "now() - toIntervalSecond(absolute_delay)",
		"log_pointer <= log_max_index OR inserts_in_queue > 0 OR absolute_delay > 0",
		"database = 'db' AND table IN ('tempo_traces', 'tempo_traces_touched')",
	} {
		if !strings.Contains(q, want) {
			t.Errorf("replica since lacks %q:\n%s", want, q)
		}
	}
}

func TestEachPassReadsTheEarlierPassesReads(t *testing.T) {
	r := &recorder{running: running0, frontier: frontier}
	if _, err := r.run(cursor0); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(r.got[1], " reads") {
		t.Errorf("first batch %q, want no reads", r.got[1])
	}
	r.running = running0.Add(time.Minute)
	if _, err := r.run(cursor0); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(r.got[4], " reads 2026-10-29T10:02:01Z 2026-10-29T10:02:00Z") {
		t.Errorf("second batch %q, want the first pass's read", r.got[4])
	}
	r.running = running0.Add(2 * time.Minute)
	if _, err := r.run(cursor0); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(r.got[7], " reads 2026-10-29T10:02:01Z 2026-10-29T10:02:00Z 2026-10-29T10:03:01Z 2026-10-29T10:03:00Z") {
		t.Errorf("third batch %q, want both earlier reads, oldest first", r.got[7])
	}
}

func TestAPassReturnsItsReadUnlessItTookNone(t *testing.T) {
	took := RunningRead{At: running0.Add(time.Second), Start: running0}
	for _, c := range []struct {
		fail        string
		running, at time.Time
		want        RunningRead
	}{
		{"running", running0, time.Time{}, RunningRead{}},
		{"", time.Unix(0, 0), time.Time{}, RunningRead{}},
		{"", running0, time.Unix(0, 0), RunningRead{}},
		{"batch", running0, time.Time{}, took},
		{"frontier", running0, time.Time{}, took},
		{"number", running0, time.Time{}, took},
	} {
		r := &recorder{n: 3, frontier: frontier, fail: c.fail, running: c.running, at: c.at}
		if _, err := r.run(cursor0); err == nil || r.last != c.want {
			t.Errorf("%q failed: read %v, err %v; want %v and the error", c.fail, r.last, err, c.want)
		}
	}
}

// readsOf returns the reads of adding rs in turn.
func readsOf(rs ...RunningRead) Reads {
	var r Reads
	for _, read := range rs {
		r = r.add(read)
	}
	return r
}

func minuteRead(i int) RunningRead {
	at := running0.Add(time.Duration(i) * time.Minute)
	return RunningRead{At: at, Start: at}
}

func TestReadsKeepTheNewestAndTheOldestWithinEachPowerOfTwoReads(t *testing.T) {
	var r Reads
	for i := range 10 {
		r = r.add(minuteRead(i))
		r = r.add(RunningRead{})
	}
	want := []RunningRead{minuteRead(0), minuteRead(4), minuteRead(6), minuteRead(7), minuteRead(8), minuteRead(9)}
	if !slices.Equal(r.all(), want) {
		t.Errorf("reads %v, want %v", r.all(), want)
	}
}

func TestReadsKeepAReadAtMostThreeTimesAnyAgeUpTo1024Old(t *testing.T) {
	const span = 1024
	var r Reads
	for n := 0; n < 12*span; n++ {
		r = r.add(minuteRead(n))
		kept := r.all()
		if len(kept) > readBuckets+1 {
			t.Fatalf("after %d reads, %d kept, want at most %d", n+1, len(kept), readBuckets+1)
		}
		// kept is oldest first, so ages run from oldest to youngest.
		i := len(kept) - 1
		for age := 1; age <= min(span, n); age++ {
			for i >= 0 && n-int(kept[i].At.Sub(running0)/time.Minute) < age {
				i--
			}
			if i < 0 {
				t.Fatalf("after %d reads, no read is at least %d old", n+1, age)
			}
			if youngest := n - int(kept[i].At.Sub(running0)/time.Minute); youngest > 3*age {
				t.Fatalf("after %d reads, the youngest read at least %d old is %d old, want at most %d", n+1, age, youngest, 3*age)
			}
		}
	}
}

func TestReadsDoNotShareTheirArrays(t *testing.T) {
	r := readsOf(minuteRead(0), minuteRead(1), minuteRead(2))
	old := slices.Clone(r.all())
	a := r.add(minuteRead(3))
	b := r.add(minuteRead(4))
	if got := a.all(); got[len(got)-1] != minuteRead(3) {
		t.Errorf("a later add from the same reads changed the newest read of an earlier one to %v", got[len(got)-1])
	}
	if got := b.all(); got[len(got)-1] != minuteRead(4) {
		t.Errorf("newest read %v, want %v", got[len(got)-1], minuteRead(4))
	}
	if !slices.Equal(r.all(), old) {
		t.Errorf("add changed its receiver to %v, want %v", r.all(), old)
	}
}

func bothStatements() map[string]statements {
	return map[string]statements{
		"local":   localStatements(time.Second, 10, replica{delay: "0", since: "NULL"}),
		"cluster": clusterStatements("c", "db", time.Second, 10),
	}
}

func TestTheBatchReadsTheNumberingVersionsNotTheNestedSetHistory(t *testing.T) {
	for name, st := range bothStatements() {
		q := st.batch("p1", time.Time{}, RunningRead{}, Reads{})
		if !strings.Contains(q, "tempo_traces_numbered") || regexp.MustCompile(`tempo_traces_nested_set\b`).MatchString(q) {
			t.Errorf("%s batch reads the nested-set history or no numbering versions:\n%s", name, q)
		}
		if !strings.Contains(q, "toUUID('p1') AS pass") {
			t.Errorf("%s batch does not write its pass id:\n%s", name, q)
		}
	}
}

func TestTheNumberingReadsOnlyItsBatch(t *testing.T) {
	for name, st := range bothStatements() {
		q := st.number("p1")
		if !strings.Contains(q, "tempo_traces_nested_set_batch WHERE pass = toUUID('p1')") || strings.Contains(q, "tempo_traces_touched") {
			t.Errorf("%s numbering does not read only batch p1:\n%s", name, q)
		}
	}
}

func TestPassStatementsDoNotReadTenants(t *testing.T) {
	for name, st := range bothStatements() {
		if q := st.batch("p1", time.Time{}, RunningRead{}, Reads{}) + st.number("p1"); regexp.MustCompile(`\boid\b`).MatchString(q) {
			t.Errorf("%s pass reads oid:\n%s", name, q)
		}
	}
}

func TestClusterStatementsRunOnEachShardsFirstReplica(t *testing.T) {
	st := clusterStatements("c", "db", time.Second, 10)
	for target, q := range map[string]string{
		"`db`.tempo_traces_nested_set_batch_dist": st.batch("p1", time.Time{}, RunningRead{}, Reads{}),
		"`db`.tempo_traces_nested_set_dist":       st.number("p1"),
	} {
		for _, want := range []string{
			"INSERT INTO " + target + " ", "FROM cluster('c', view(",
			"parallel_distributed_insert_select = 2", "load_balancing = 'in_order'", "prefer_localhost_replica = 0",
		} {
			if !strings.Contains(q, want) {
				t.Errorf("statement into %s lacks %q:\n%s", target, want, q)
			}
		}
	}
}
