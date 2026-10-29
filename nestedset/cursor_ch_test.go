package nestedset

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/qryn/v5/ctrl/logger"
	"github.com/metrico/qryn/v5/ctrl/qryn/maintenance"
	"github.com/metrico/qryn/v5/shared/chtest"
)

// addTouches appends touches at fixed times: trace -> unix ms.
func addTouches(t *testing.T, conn clickhouse.Conn, table string, at map[int]int64) {
	t.Helper()
	var rows []string
	for trace, ms := range at {
		rows = append(rows, fmt.Sprintf("(%s, fromUnixTimestamp64Milli(toInt64(%d)))", traceID(trace), ms))
	}
	exec(t, conn, "INSERT INTO "+table+" (trace_id, inserted_at) VALUES "+strings.Join(rows, ", "))
}

func serverNow(t *testing.T, conn clickhouse.Conn) time.Time {
	t.Helper()
	var now time.Time
	if err := conn.QueryRow(context.Background(), "SELECT now64(3)").Scan(&now); err != nil {
		t.Fatal(err)
	}
	return now
}

func passFrom(t *testing.T, conn clickhouse.Conn, idle time.Duration, maxSpans int, cursor time.Time) time.Time {
	t.Helper()
	next, _, err := Pass(context.Background(), conn, idle, maxSpans, cursor, Reads{})
	if err != nil {
		t.Fatal(err)
	}
	return next
}

// versions returns the distinct numbering versions of a trace.
func versions(t *testing.T, conn clickhouse.Conn, table string, trace int) []uint64 {
	t.Helper()
	rows, err := conn.Query(context.Background(), fmt.Sprintf(
		"SELECT DISTINCT version FROM %s WHERE trace_id = %s ORDER BY version", table, traceID(trace)))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []uint64
	for rows.Next() {
		var v uint64
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	return out
}

func TestCursorIdleTraceWaitsAndIsNotSkipped(t *testing.T) {
	conn := open(t)
	insert(t, conn, chain(1, 2)...)
	touch(t, conn, map[int]time.Duration{1: 2 * time.Second})
	start := serverNow(t, conn).Add(-time.Hour)
	c := passFrom(t, conn, 10*time.Second, 1000, start)
	if got := numberedTraces(t, conn, 1); len(got) != 0 {
		t.Fatalf("numbered %v before the idle period", got)
	}
	if lead := serverNow(t, conn).Add(-10 * time.Second); c.After(lead) {
		t.Errorf("empty-batch frontier %v leads now - idle %v", c, lead)
	}
	c2 := passFrom(t, conn, time.Millisecond, 1000, c)
	if got := numberedTraces(t, conn, 1); fmt.Sprint(got) != "[1]" {
		t.Errorf("numbered %v after the idle period from cursor %v, want [1]", got, c)
	}
	if c2.Before(c) {
		t.Errorf("cursor went back from %v to %v", c, c2)
	}
}

func TestCursorReplicaDelayGuardStillApplies(t *testing.T) {
	conn := open(t)
	insert(t, conn, chain(1, 2)...)
	touch(t, conn, map[int]time.Duration{1: 3 * time.Second})
	start := serverNow(t, conn).Add(-time.Hour)
	c, _, err := run(context.Background(), connInserter(conn), connReader(conn), localStatements(0, 1000, replica{delay: "toUInt64(5)", since: "NULL"}), start, Reads{})
	if err != nil {
		t.Fatal(err)
	}
	if got := numberedTraces(t, conn, 1); len(got) != 0 {
		t.Fatalf("numbered %v within delay + 2s", got)
	}
	if lead := serverNow(t, conn).Add(-7 * time.Second); c.After(lead) {
		t.Errorf("frontier %v leads now - delay - 2s %v", c, lead)
	}
	if _, _, err := run(context.Background(), connInserter(conn), connReader(conn), localStatements(0, 1000, replica{delay: "toUInt64(0)", since: "NULL"}), c, Reads{}); err != nil {
		t.Fatal(err)
	}
	if got := numberedTraces(t, conn, 1); fmt.Sprint(got) != "[1]" {
		t.Errorf("numbered %v once the replica caught up, want [1]", got)
	}
}

func TestCursorAReplicaWithEntriesToApplyCapsTheCutoffAtTheNewestStartReadBeforeThem(t *testing.T) {
	conn := open(t)
	now := serverNow(t, conn)
	cursor := now.Add(-time.Hour)
	read := func(at, start time.Duration) RunningRead { return RunningRead{At: now.Add(at), Start: now.Add(start)} }
	reads := readsOf(read(-50*time.Second, -55*time.Second), read(-30*time.Second, -40*time.Second), read(-10*time.Second, -12*time.Second))
	since := func(d time.Duration) string { return fmt.Sprintf("toDateTime64(%d, 0)", now.Add(d).Unix()) }
	for _, c := range []struct {
		name, since string
		reads       Reads
		want        time.Time
	}{
		{"reads before the entries", since(-20 * time.Second), reads, now.Add(-40 * time.Second)},
		{"reads after the entries", since(-60 * time.Second), reads, cursor},
		{"no reads", since(-20 * time.Second), Reads{}, cursor},
		{"a read older than the cursor", since(-20 * time.Second), readsOf(RunningRead{At: cursor.Add(-time.Minute), Start: cursor.Add(-2 * time.Minute)}), cursor},
	} {
		st := localStatements(0, 1000, replica{delay: "toUInt64(0)", since: c.since})
		got, _, err := run(context.Background(), connInserter(conn), connReader(conn), st, cursor, c.reads)
		if err != nil {
			t.Fatal(err)
		}
		if !got.Equal(c.want) {
			t.Errorf("%s: cursor %v, want %v", c.name, got, c.want)
		}
	}
	st := localStatements(0, 1000, replica{delay: "toUInt64(0)", since: "NULL"})
	got, _, err := run(context.Background(), connInserter(conn), connReader(conn), st, cursor, reads)
	if err != nil {
		t.Fatal(err)
	}
	if got.Before(now) {
		t.Errorf("caught up: cursor %v, want about now (%v)", got, now)
	}
}

func TestCursorReplicaSinceNeitherWrapsNorReachesBackBeforeTheNodesStart(t *testing.T) {
	conn := open(t)
	var started time.Time
	if err := conn.QueryRow(context.Background(), "SELECT now() - uptime()").Scan(&started); err != nil {
		t.Fatal(err)
	}
	const columns = "log_pointer UInt64, log_max_index UInt64, last_queue_update DateTime, inserts_in_queue UInt32, " +
		"inserts_oldest_time DateTime, absolute_delay UInt64"
	for _, c := range []struct {
		name, row string
		lo, hi    time.Time
	}{
		{"an unpulled entry and no queue update since the start", "(0, 0, 0, 0, 0, 0)", started.Add(-3 * time.Second), started.Add(-time.Second)},
		{"no queue update ever", "(1, 1, 0, 0, 0, toUInt64(now()))", time.Unix(-3, 0), time.Unix(0, 0)},
		{"a queued insert of the epoch", "(2, 1, 0, 1, 0, 0)", time.Unix(-3, 0), time.Unix(0, 0)},
		{"caught up", "(2, 1, 0, 0, 0, 0)", time.Time{}, time.Time{}},
	} {
		var got *time.Time
		q := "SELECT " + replicaSince("values('"+columns+"', "+c.row+")")
		if err := conn.QueryRow(context.Background(), q).Scan(&got); err != nil {
			t.Fatalf("%s: %v\n%s", c.name, err, q)
		}
		switch {
		case c.lo.IsZero() && got != nil:
			t.Errorf("%s: since %v, want NULL", c.name, *got)
		case !c.lo.IsZero() && (got == nil || got.Before(c.lo) || got.After(c.hi)):
			t.Errorf("%s: since %v, want between %v and %v", c.name, got, c.lo, c.hi)
		}
	}
}

func TestCursorRetouchedTraceIsRenumberedOnce(t *testing.T) {
	conn := open(t)
	insert(t, conn, chain(1, 2)...)
	touch(t, conn, map[int]time.Duration{1: time.Minute})
	c := passFrom(t, conn, time.Second, 1000, serverNow(t, conn).Add(-time.Hour))
	time.Sleep(100 * time.Millisecond)
	addTouches(t, conn, "tempo_traces_touched", map[int]int64{1: serverNow(t, conn).UnixMilli()})
	time.Sleep(100 * time.Millisecond)
	for range 3 {
		c = passFrom(t, conn, time.Millisecond, 1000, c)
	}
	if v := versions(t, conn, "tempo_traces_nested_set", 1); len(v) != 2 {
		t.Errorf("versions %v, want the first numbering and exactly one renumbering", v)
	}
}

func TestCursorEqualTimestampsAtTheCursorLoseNothing(t *testing.T) {
	conn := open(t)
	at := serverNow(t, conn).Add(-time.Minute).UnixMilli()
	touches := map[int]int64{}
	for trace := 1; trace <= 4; trace++ {
		insert(t, conn, chain(trace, 2)...)
		touches[trace] = at
	}
	exec(t, conn, "TRUNCATE TABLE tempo_traces_touched")
	addTouches(t, conn, "tempo_traces_touched", touches)
	c := serverNow(t, conn).Add(-time.Hour)
	var order []int
	for range 4 {
		before := numberedTraces(t, conn, 1, 2, 3, 4)
		c = passFrom(t, conn, time.Second, 2, c)
		if c.UnixMilli() != at {
			t.Errorf("frontier %d, want the shared touch %d", c.UnixMilli(), at)
		}
		for _, tr := range numberedTraces(t, conn, 1, 2, 3, 4) {
			if !slices.Contains(before, tr) {
				order = append(order, tr)
			}
		}
	}
	if fmt.Sprint(order) != "[1 2 3 4]" {
		t.Errorf("numbered in order %v, want one per pass [1 2 3 4]", order)
	}
}

func TestCursorCapStopsMidBacklogAndTheNextPassContinues(t *testing.T) {
	conn := open(t)
	base := serverNow(t, conn).Add(-10 * time.Minute).UnixMilli()
	touches := map[int]int64{}
	for trace := 1; trace <= 10; trace++ {
		insert(t, conn, chain(trace, 2)...)
		touches[trace] = base + int64(trace)*1000
	}
	exec(t, conn, "TRUNCATE TABLE tempo_traces_touched")
	addTouches(t, conn, "tempo_traces_touched", touches)
	c := time.Time{}
	var frontiers []int64
	for range 6 {
		c = passFrom(t, conn, time.Second, 4, c)
		frontiers = append(frontiers, c.UnixMilli()-base)
	}
	if got := numberedTraces(t, conn, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10); len(got) != 10 {
		t.Errorf("numbered %v, want all 10", got)
	}
	// Two traces per pass: frontiers at traces 2, 4, 6, 8, 10, then the cutoff.
	if fmt.Sprint(frontiers[:5]) != "[2000 4000 6000 8000 10000]" || frontiers[5] < 10*60*1000-2000 {
		t.Errorf("frontiers (ms after base) %v", frontiers)
	}
}

func TestCursorDoesNotAdvanceWhenTheNumberingFails(t *testing.T) {
	conn := open(t)
	insert(t, conn, chain(1, 2)...)
	touch(t, conn, map[int]time.Duration{1: time.Minute})
	start := serverNow(t, conn).Add(-time.Hour)
	st := localStatements(time.Second, 1000, replica{delay: "toUInt64(0)", since: "NULL"})
	st.number = func(string) string { return "INSERT INTO no_such_table SELECT 1" }
	c, _, err := run(context.Background(), connInserter(conn), connReader(conn), st, start, Reads{})
	if err == nil {
		t.Fatal("want the numbering error")
	}
	if !c.Equal(start) {
		t.Errorf("cursor %v after a failed numbering, want %v", c, start)
	}
	passFrom(t, conn, time.Second, 1000, c)
	if got := numberedTraces(t, conn, 1); fmt.Sprint(got) != "[1]" {
		t.Errorf("numbered %v on the retry, want [1]", got)
	}
}

func TestCursorLateSpanRenumbersTheWholeTrace(t *testing.T) {
	conn := open(t)
	insert(t, conn, chain(1, 3)...)
	time.Sleep(100 * time.Millisecond)
	c := passFrom(t, conn, time.Millisecond, 1000, time.Time{})
	insert(t, conn, span{1, 4, 3, 4})
	time.Sleep(100 * time.Millisecond)
	passFrom(t, conn, time.Millisecond, 1000, c)
	v := versions(t, conn, "tempo_traces_nested_set", 1)
	if len(v) != 2 {
		t.Fatalf("versions %v, want 2", v)
	}
	var n uint64
	if err := conn.QueryRow(context.Background(), fmt.Sprintf(
		"SELECT count() FROM tempo_traces_nested_set WHERE trace_id = %s AND version = %d", traceID(1), v[1])).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Errorf("the late span rewrote %d rows, want the whole trace (4)", n)
	}
	assertNumbers(t, conn, 1, map[int]numbers{1: {1, 8, -1}, 2: {2, 7, 1}, 3: {3, 6, 2}, 4: {4, 5, 3}})
}

func TestCursorFailoverContinuesFromThePreviousTerm(t *testing.T) {
	conn := open(t)
	store := ConnStore{Conn: conn}
	insert(t, conn, chain(1, 2)...)
	insert(t, conn, chain(2, 2)...)
	base := serverNow(t, conn).Add(-5 * time.Minute).UnixMilli()
	exec(t, conn, "TRUNCATE TABLE tempo_traces_touched")
	addTouches(t, conn, "tempo_traces_touched", map[int]int64{1: base, 2: base + 1000})

	a := &elector{store: store, me: "a", leaseLen: 3 * time.Second, log: &logs{}, passOK: true}
	if !a.step(context.Background()) {
		t.Fatal("a did not win the first term")
	}
	if err := a.pass(context.Background(), Config{Idle: time.Second, MaxSpans: 2}); err != nil {
		t.Fatal(err)
	}
	if !a.step(context.Background()) {
		t.Fatal("a did not renew")
	}
	if a.cursor.UnixMilli() != base {
		t.Fatalf("a's cursor %d, want trace 1's touch %d", a.cursor.UnixMilli(), base)
	}
	time.Sleep(4 * time.Second)

	b := &elector{store: store, me: "b", leaseLen: time.Minute, log: &logs{}, passOK: true}
	if !b.step(context.Background()) {
		t.Fatal("b did not take over")
	}
	if !b.cursor.Equal(a.cursor) {
		t.Errorf("b took cursor %v, want a's %v", b.cursor, a.cursor)
	}
	var claimCursor int64
	if err := conn.QueryRow(context.Background(),
		"SELECT toUnixTimestamp64Milli(cursor) FROM tempo_traces_nested_set_lease WHERE claim ORDER BY term DESC LIMIT 1").Scan(&claimCursor); err != nil {
		t.Fatal(err)
	}
	if claimCursor != base {
		t.Errorf("the new term's claim carries cursor %d, want %d", claimCursor, base)
	}
	if _, _, err := store.Pass(context.Background(), time.Second, 2, b.cursor, Reads{}); err != nil {
		t.Fatal(err)
	}
	if got := numberedTraces(t, conn, 1, 2); fmt.Sprint(got) != "[1 2]" {
		t.Errorf("numbered %v after failover, want [1 2]", got)
	}
}

// openCluster is chtest.OpenCluster with the schema and the host and port of
// a shard other than the connected one.
func openCluster(t *testing.T) (clickhouse.Conn, string, string, string) {
	t.Helper()
	conn, db, cluster := chtest.OpenCluster(t)
	mode := maintenance.CLUST_MODE_SINGLE | maintenance.CLUST_MODE_DISTRIBUTED
	if err := maintenance.Update(conn, db, cluster, mode, 7, "", "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	var host string
	var port uint16
	if err := conn.QueryRow(context.Background(), fmt.Sprintf(
		"SELECT host_name, port FROM system.clusters WHERE cluster = '%s' AND is_local = 0 AND shard_num != "+
			"(SELECT shard_num FROM system.clusters WHERE cluster = '%s' AND is_local LIMIT 1) LIMIT 1",
		cluster, cluster)).Scan(&host, &port); err != nil {
		t.Fatal(err)
	}
	return conn, db, cluster, fmt.Sprintf("%s:%d", host, port)
}

func TestCursorEmptyShardDoesNotPassABackloggedShard(t *testing.T) {
	conn, db, cluster, remote := openCluster(t)
	spans := fmt.Sprintf("FUNCTION remote('%s', %s, tempo_traces)", remote, db)
	touched := fmt.Sprintf("FUNCTION remote('%s', %s, tempo_traces_touched)", remote, db)
	base := serverNow(t, conn).Add(-10 * time.Minute).UnixMilli()
	touches := map[int]int64{}
	for trace := 1; trace <= 6; trace++ {
		insertInto(t, conn, spans, chain(trace, 2)...)
		touches[trace] = base + int64(trace)*1000
	}
	exec(t, conn, fmt.Sprintf("TRUNCATE TABLE %s.tempo_traces_touched ON CLUSTER '%s' SYNC", db, cluster))
	addTouches(t, conn, touched, touches)

	store := ClusterStore{Conn: conn, Cluster: cluster, Database: db}
	c := time.Time{}
	var frontiers []int64
	for range 4 {
		next, _, err := store.Pass(context.Background(), time.Second, 4, c, Reads{})
		if err != nil {
			t.Fatal(err)
		}
		c = next
		frontiers = append(frontiers, c.UnixMilli()-base)
	}
	// The connected shard is empty, its frontier is its cutoff; the minimum is
	// the backlogged shard's frontier until it catches up.
	if fmt.Sprint(frontiers[:3]) != "[2000 4000 6000]" || frontiers[3] < 10*60*1000-2000 {
		t.Errorf("cluster cursors (ms after base) %v", frontiers)
	}
	var n uint64
	if err := conn.QueryRow(context.Background(), fmt.Sprintf(
		"SELECT uniqExact(trace_id) FROM clusterAllReplicas('%s', %s.tempo_traces_nested_set)", cluster, db)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 6 {
		t.Errorf("numbered %d traces on the backlogged shard, want 6", n)
	}
}

func TestCursorClusterStatementTwoFailureKeepsTheCursor(t *testing.T) {
	conn, db, cluster, remote := openCluster(t)
	insertInto(t, conn, fmt.Sprintf("FUNCTION remote('%s', %s, tempo_traces)", remote, db), chain(1, 2)...)
	time.Sleep(100 * time.Millisecond)
	st := clusterStatements(cluster, db, time.Millisecond, 1000)
	st.number = func(string) string { return "INSERT INTO no_such_table SELECT 1" }
	start := serverNow(t, conn).Add(-time.Hour)
	c, _, err := run(context.Background(), connInserter(conn), connReader(conn), st, start, Reads{})
	if err == nil || !c.Equal(start) {
		t.Fatalf("cursor %v, err %v; want %v and the numbering error", c, err, start)
	}
}

func TestCursorUnmergedTouchesAcrossMinutesUseTheNewest(t *testing.T) {
	conn := open(t)
	insert(t, conn, chain(1, 2)...)
	now := serverNow(t, conn)
	exec(t, conn, "TRUNCATE TABLE tempo_traces_touched")
	for _, age := range []time.Duration{10 * time.Minute, 5 * time.Minute, 5 * time.Second} {
		addTouches(t, conn, "tempo_traces_touched", map[int]int64{1: now.Add(-age).UnixMilli()})
	}
	c := passFrom(t, conn, 30*time.Second, 1000, now.Add(-2*time.Minute))
	if got := numberedTraces(t, conn, 1); len(got) != 0 {
		t.Fatalf("numbered %v though the newest touch is 5s old", got)
	}
	passFrom(t, conn, time.Second, 1000, c)
	if got := numberedTraces(t, conn, 1); fmt.Sprint(got) != "[1]" {
		t.Errorf("numbered %v, want [1]", got)
	}
}

func TestCursorNumberingsAreReadFromTheCursorsMinute(t *testing.T) {
	conn := open(t)
	cursor := serverNow(t, conn).Add(-5 * time.Minute).Truncate(time.Minute).Add(30 * time.Second)
	exec(t, conn, "TRUNCATE TABLE tempo_traces_touched")
	numberings := map[int]int64{
		1: cursor.Add(-2 * time.Minute).UnixMilli(),    // before the cursor's minute
		2: cursor.Add(20 * time.Second).UnixMilli(),    // after the touch
		3: cursor.Add(-time.Millisecond).UnixMilli(),   // in the cursor's minute, before the cursor
		4: cursor.Add(-24 * time.Hour / 2).UnixMilli(), // hours before the cursor
	}
	for trace, version := range numberings {
		insert(t, conn, chain(trace, 2)...)
		exec(t, conn, fmt.Sprintf("INSERT INTO tempo_traces_numbered (trace_id, version) VALUES (%s, %d)", traceID(trace), version))
	}
	exec(t, conn, "TRUNCATE TABLE tempo_traces_touched")
	addTouches(t, conn, "tempo_traces_touched", map[int]int64{
		1: cursor.Add(10 * time.Second).UnixMilli(), 2: cursor.Add(10 * time.Second).UnixMilli(),
		3: cursor.Add(10 * time.Second).UnixMilli(), 4: cursor.Add(10 * time.Second).UnixMilli(),
	})
	passFrom(t, conn, time.Second, 1000, cursor)
	if got := numberedTraces(t, conn, 1, 2, 3, 4); fmt.Sprint(got) != "[1 3 4]" {
		t.Errorf("numbered %v, want [1 3 4]: only trace 2's numbering is newer than its touch", got)
	}
}

// slowInsert inserts one root span of each of traces, one row every interval,
// into table through conn, and returns a channel that gets the insert's error.
// A Distributed table forwards in the foreground when foreground is set.
func slowInsert(conn clickhouse.Conn, table string, foreground bool, interval time.Duration, traces ...int) <-chan error {
	ids := make([]string, len(traces))
	for i, trace := range traces {
		ids[i] = traceID(trace)
	}
	fg := 0
	if foreground {
		fg = 1
	}
	q := fmt.Sprintf(`INSERT INTO %s (oid, trace_id, span_id, parent_id, timestamp_ns)
		SELECT '0', [%s][number + 1], %s, '', 1790812800000000000 FROM numbers(%d)
		WHERE sleepEachRow(%f) = 0
		SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, distributed_foreground_insert = %d`,
		table, strings.Join(ids, ", "), spanID(1), len(traces), interval.Seconds(), fg)
	done := make(chan error, 1)
	go func() { done <- conn.Exec(context.Background(), q) }()
	return done
}

func TestCursorARunningInsertHoldsTheCutoffAtItsStart(t *testing.T) {
	conn := open(t)
	before := serverNow(t, conn)
	done := slowInsert(conn, "tempo_traces", true, 600*time.Millisecond, 100, 101, 102, 103, 104, 105, 106, 107)
	time.Sleep(2500 * time.Millisecond)
	insert(t, conn, chain(2, 2)...)
	touched := serverNow(t, conn)
	time.Sleep(100 * time.Millisecond)
	c := passFrom(t, conn, time.Millisecond, 1000, before.Add(-time.Hour))
	if got := numberedTraces(t, conn, 2, 100, 101); len(got) != 0 {
		t.Errorf("numbered %v, touched after the running insert started", got)
	}
	if c.Before(before.Add(-time.Second)) || !c.Before(touched.Add(-time.Second)) {
		t.Errorf("frontier %v, want about the running insert's start, after %v", c, before)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	passFrom(t, conn, time.Millisecond, 1000, c)
	if got := numberedTraces(t, conn, 2, 100, 107); fmt.Sprint(got) != "[2 100 107]" {
		t.Errorf("numbered %v once the insert ended, want [2 100 107]", got)
	}
}

// clusterNumbered returns which of traces have nested-set rows on any shard.
func clusterNumbered(t *testing.T, conn clickhouse.Conn, cluster, db string, traces ...int) []int {
	t.Helper()
	var got []int
	for _, trace := range traces {
		var n uint64
		if err := conn.QueryRow(context.Background(), fmt.Sprintf(
			"SELECT count() FROM clusterAllReplicas('%s', %s.tempo_traces_nested_set) WHERE trace_id = %s",
			cluster, db, traceID(trace))).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			got = append(got, trace)
		}
	}
	return got
}

func TestCursorARunningDistributedInsertHoldsEachShardsCutoff(t *testing.T) {
	for name, foreground := range map[string]bool{"foreground": true, "background": false} {
		t.Run(name, func(t *testing.T) {
			conn, db, cluster, remote := openCluster(t)
			var slow []int
			for trace := 100; trace < 116; trace++ {
				slow = append(slow, trace)
			}
			done := slowInsert(conn, "tempo_traces_dist", foreground, 300*time.Millisecond, slow...)
			time.Sleep(2000 * time.Millisecond)
			insertInto(t, conn, fmt.Sprintf("FUNCTION remote('%s', %s, tempo_traces)", remote, db), chain(2, 2)...)
			insert(t, conn, chain(3, 2)...)
			time.Sleep(100 * time.Millisecond)
			store := ClusterStore{Conn: conn, Cluster: cluster, Database: db}
			c, _, err := store.Pass(context.Background(), time.Millisecond, 1000, serverNow(t, conn).Add(-time.Hour), Reads{})
			if err != nil {
				t.Fatal(err)
			}
			if got := clusterNumbered(t, conn, cluster, db, 2, 3); len(got) != 0 {
				t.Errorf("numbered %v, touched on each shard after the running insert started", got)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			exec(t, conn, "SYSTEM FLUSH DISTRIBUTED tempo_traces_dist")
			time.Sleep(100 * time.Millisecond)
			if _, _, err := store.Pass(context.Background(), time.Millisecond, 1000, c, Reads{}); err != nil {
				t.Fatal(err)
			}
			if got := clusterNumbered(t, conn, cluster, db, append([]int{2, 3}, slow...)...); len(got) != 2+len(slow) {
				t.Errorf("numbered %v once the insert ended, want 2, 3 and %v", got, slow)
			}
		})
	}
}

func TestLeaseReadCarriesTheCursorOfTheLatestRowByTermAndExpiry(t *testing.T) {
	conn := open(t)
	base := serverNow(t, conn).Add(-time.Hour).Truncate(time.Second)
	ms := func(d time.Duration) int64 { return base.Add(d).UnixMilli() }
	exec(t, conn, fmt.Sprintf(`INSERT INTO tempo_traces_nested_set_lease (term, holder, claim, expires_at, cursor) VALUES
		(4, 'x', true, fromUnixTimestamp64Milli(toInt64(%d)), fromUnixTimestamp64Milli(toInt64(%d))),
		(5, 'old', true, fromUnixTimestamp64Milli(toInt64(%d)), fromUnixTimestamp64Milli(toInt64(%d))),
		(5, 'old', false, fromUnixTimestamp64Milli(toInt64(%d)), fromUnixTimestamp64Milli(toInt64(%d))),
		(5, 'old', false, fromUnixTimestamp64Milli(toInt64(%d)), fromUnixTimestamp64Milli(toInt64(%d))),
		(5, 'old', false, fromUnixTimestamp64Milli(toInt64(%d)), 0)`,
		ms(10*time.Second), ms(9*time.Minute),
		ms(20*time.Second), ms(time.Minute),
		ms(25*time.Second), ms(8*time.Minute),
		ms(30*time.Second), ms(2*time.Minute),
		ms(31*time.Second)))
	rows, _, err := ConnStore{Conn: conn}.ReadLease(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := inheritedCursor(rows); got.UnixMilli() != ms(2*time.Minute) {
		t.Errorf("inherited cursor %v, want term 5's latest renewal's %v", got, base.Add(2*time.Minute))
	}
}

func TestCursorANodeWithoutThePassesFrontierRowFailsTheNumberingAndTheRead(t *testing.T) {
	conn := open(t)
	insert(t, conn, chain(1, 2)...)
	time.Sleep(100 * time.Millisecond)
	st := localStatements(time.Millisecond, 1000, replica{delay: tables{}.replicaDelay(time.Millisecond), since: tables{}.replicaSince()})
	if err := conn.Exec(context.Background(), st.batch("00000000-0000-0000-0000-000000000001", time.Time{}, RunningRead{At: serverNow(t, conn), Start: serverNow(t, conn)}, Reads{})); err != nil {
		t.Fatal(err)
	}
	other := "00000000-0000-0000-0000-000000000002"
	exec(t, conn, "INSERT INTO tempo_traces_nested_set_batch (pass, trace_id) VALUES ('"+other+"', "+traceID(1)+")")
	if err := conn.Exec(context.Background(), st.number(other)); err == nil || !strings.Contains(err.Error(), "no frontier row") {
		t.Errorf("numbering a batch without its frontier row: err %v, want the missing frontier row", err)
	}
	var n uint64
	var f time.Time
	if err := conn.QueryRow(context.Background(), st.frontier(other)).Scan(&n, &f); err == nil || !strings.Contains(err.Error(), "no frontier row") {
		t.Errorf("frontier read without a frontier row: err %v, want the missing frontier row", err)
	}
	if got := numberedTraces(t, conn, 1); len(got) != 0 {
		t.Errorf("numbered %v from a batch without its frontier row", got)
	}
}

func TestCursorTheZeroTraceIDIsNeverNumbered(t *testing.T) {
	conn := open(t)
	insert(t, conn, chain(0, 2)...)
	time.Sleep(100 * time.Millisecond)
	touched := serverNow(t, conn)
	c := passFrom(t, conn, time.Millisecond, 1000, touched.Add(-time.Hour))
	if got := numberedTraces(t, conn, 0); len(got) != 0 {
		t.Errorf("numbered the zero trace id")
	}
	insert(t, conn, chain(1, 2)...)
	time.Sleep(100 * time.Millisecond)
	passFrom(t, conn, time.Millisecond, 1000, c)
	if got := numberedTraces(t, conn, 0, 1); fmt.Sprint(got) != "[1]" {
		t.Errorf("numbered %v, want [1]", got)
	}
	if !c.After(touched) {
		t.Errorf("cursor %v held by the zero trace id, touched before %v", c, touched)
	}
}

func TestCursorARunningInsertIntoAnotherDatabaseDoesNotHoldTheCutoff(t *testing.T) {
	conn := open(t)
	other, otherDB := chtest.Open(t)
	var db string
	if err := conn.QueryRow(context.Background(), "SELECT currentDatabase()").Scan(&db); err != nil {
		t.Fatal(err)
	}
	exec(t, other, "CREATE TABLE tempo_traces AS "+db+".tempo_traces")
	unqualified := slowInsert(other, "tempo_traces", true, 300*time.Millisecond, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109)
	qualified := slowInsert(conn, otherDB+".tempo_traces", true, 300*time.Millisecond, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109)
	time.Sleep(time.Second)
	insert(t, conn, chain(2, 2)...)
	time.Sleep(100 * time.Millisecond)
	passFrom(t, conn, time.Millisecond, 1000, serverNow(t, conn).Add(-time.Hour))
	if got := numberedTraces(t, conn, 2); fmt.Sprint(got) != "[2]" {
		t.Errorf("numbered %v while inserts into %s ran, want [2]", got, otherDB)
	}
	for _, done := range []<-chan error{unqualified, qualified} {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

// openReplicatedCluster is chtest.OpenReplicatedCluster with the replicated
// schema and the host and port of a replica of the connected node's shard
// other than its first, which runs the passes.
func openReplicatedCluster(t *testing.T) (clickhouse.Conn, string, string, string) {
	t.Helper()
	conn, db, cluster := chtest.OpenReplicatedCluster(t)
	mode := maintenance.CLUST_MODE_CLOUD | maintenance.CLUST_MODE_DISTRIBUTED
	if err := maintenance.Update(conn, db, cluster, mode, 7, "", "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	var host string
	var port uint16
	if err := conn.QueryRow(context.Background(), fmt.Sprintf(
		"SELECT host_name, port FROM system.clusters WHERE cluster = '%s' AND replica_num > 1 AND shard_num = "+
			"(SELECT shard_num FROM system.clusters WHERE cluster = '%s' AND is_local LIMIT 1) ORDER BY replica_num LIMIT 1",
		cluster, cluster)).Scan(&host, &port); err != nil {
		t.Fatal(err)
	}
	return conn, db, cluster, fmt.Sprintf("%s:%d", host, port)
}

func TestCursorARunningInsertOnAnotherReplicaHoldsTheCutoff(t *testing.T) {
	conn, db, cluster, replica := openReplicatedCluster(t)
	spans := fmt.Sprintf("FUNCTION remote('%s', %s, tempo_traces)", replica, db)
	before := serverNow(t, conn)
	done := slowInsert(conn, spans, true, 300*time.Millisecond, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115)
	time.Sleep(2 * time.Second)
	insertInto(t, conn, spans, chain(2, 2)...)
	syncReplicas(t, conn, cluster, db)
	store := ClusterStore{Conn: conn, Cluster: cluster, Database: db}
	c, _, err := store.Pass(context.Background(), time.Millisecond, 1000, before.Add(-time.Hour), Reads{})
	if err != nil {
		t.Fatal(err)
	}
	if got := clusterNumbered(t, conn, cluster, db, 2); len(got) != 0 {
		t.Errorf("numbered %v, touched on the other replica after its running insert started", got)
	}
	if c.After(before.Add(time.Second)) {
		t.Errorf("cursor %v passed the other replica's running insert, started about %v", c, before)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	syncReplicas(t, conn, cluster, db)
	if _, _, err := store.Pass(context.Background(), time.Millisecond, 1000, c, Reads{}); err != nil {
		t.Fatal(err)
	}
	if got := clusterNumbered(t, conn, cluster, db, 2, 100, 115); fmt.Sprint(got) != "[2 100 115]" {
		t.Errorf("numbered %v once the insert ended, want [2 100 115]", got)
	}
}

// onReplicas runs SYSTEM stmt on every replica of cluster for the replicated
// tables a pass reads spans and touches from.
func onReplicas(t *testing.T, conn clickhouse.Conn, cluster, db, stmt string) {
	t.Helper()
	for _, table := range []string{"tempo_traces", "tempo_traces_touched"} {
		exec(t, conn, fmt.Sprintf("SYSTEM %s ON CLUSTER `%s` `%s`.%s", stmt, cluster, db, table))
	}
}

// syncReplicas syncs every replica of cluster and waits until none reports a
// delay for the tables a pass reads spans and touches from.
func syncReplicas(t *testing.T, conn clickhouse.Conn, cluster, db string) {
	t.Helper()
	onReplicas(t, conn, cluster, db, "SYNC REPLICA")
	waitFor(t, func() bool {
		var behind uint64
		if err := conn.QueryRow(context.Background(), fmt.Sprintf(
			"SELECT countIf(absolute_delay > 0 OR inserts_in_queue > 0 OR log_pointer <= log_max_index) "+
				"FROM clusterAllReplicas('%s', system.replicas) WHERE database = '%s' AND table IN ('tempo_traces', 'tempo_traces_touched')",
			cluster, db)).Scan(&behind); err != nil {
			t.Fatal(err)
		}
		return behind == 0
	})
}

func TestCursorUnpulledReplicationLogEntriesHoldTheCutoffForTheIdlePeriod(t *testing.T) {
	conn, db, cluster, replica := openReplicatedCluster(t)
	const idle = 10 * time.Second
	store := ClusterStore{Conn: conn, Cluster: cluster, Database: db}
	onReplicas(t, conn, cluster, db, "STOP PULLING REPLICATION LOG")
	t.Cleanup(func() { onReplicas(t, conn, cluster, db, "START PULLING REPLICATION LOG") })
	insertInto(t, conn, fmt.Sprintf("FUNCTION remote('%s', %s, tempo_traces)", replica, db), chain(1, 2)...)
	start := serverNow(t, conn)
	c, _, err := store.Pass(context.Background(), idle, 1000, start.Add(-time.Hour), Reads{})
	if err != nil {
		t.Fatal(err)
	}
	if !c.Before(start.Add(-idle - time.Second)) {
		t.Errorf("cursor %v, want about now - idle - 2s (%v) while the first replica has log entries to pull", c, start.Add(-idle-2*time.Second))
	}
	onReplicas(t, conn, cluster, db, "START PULLING REPLICATION LOG")
	syncReplicas(t, conn, cluster, db)
	start = serverNow(t, conn)
	if c, _, err = store.Pass(context.Background(), idle, 1000, c, Reads{}); err != nil {
		t.Fatal(err)
	}
	if c.Before(start.Add(-idle - time.Second)) {
		t.Errorf("cursor %v, want about now - idle (%v) once the log was pulled", c, start.Add(-idle))
	}
}

// onLocalReplica runs SYSTEM stmt on the connected node for the replicated
// tables a pass reads spans and touches from.
func onLocalReplica(t *testing.T, conn clickhouse.Conn, db, stmt string) {
	t.Helper()
	for _, table := range []string{"tempo_traces", "tempo_traces_touched"} {
		exec(t, conn, fmt.Sprintf("SYSTEM %s `%s`.%s", stmt, db, table))
	}
}

func TestCursorAnInsertCommittedOnAnotherReplicaBeforeThisOnePullsItHoldsTheCursorAtItsStart(t *testing.T) {
	conn, db, cluster, replica := openReplicatedCluster(t)
	const idle = time.Second
	store := ClusterStore{Conn: conn, Cluster: cluster, Database: db}
	before := serverNow(t, conn)
	onLocalReplica(t, conn, db, "STOP PULLING REPLICATION LOG")
	t.Cleanup(func() { onLocalReplica(t, conn, db, "START PULLING REPLICATION LOG") })
	done := slowInsert(conn, fmt.Sprintf("FUNCTION remote('%s', %s, tempo_traces)", replica, db), true, 2500*time.Millisecond, 100, 101, 107)
	time.Sleep(time.Second)
	var reads Reads
	c, read, err := store.Pass(context.Background(), idle, 1000, before.Add(-time.Hour), reads)
	if err != nil {
		t.Fatal(err)
	}
	reads = reads.add(read)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		if c, read, err = store.Pass(context.Background(), idle, 1000, c, reads); err != nil {
			t.Fatal(err)
		}
		reads = reads.add(read)
		if c.After(before) {
			t.Errorf("pass %d after the insert committed: cursor %v passed its start %v, which this replica has yet to pull", i+1, c, before)
		}
	}
	onLocalReplica(t, conn, db, "START PULLING REPLICATION LOG")
	syncReplicas(t, conn, cluster, db)
	time.Sleep(idle + 100*time.Millisecond)
	if _, _, err = store.Pass(context.Background(), idle, 1000, c, reads); err != nil {
		t.Fatal(err)
	}
	if got := clusterNumbered(t, conn, cluster, db, 100, 107); fmt.Sprint(got) != "[100 107]" {
		t.Errorf("numbered %v once this replica pulled the insert, want [100 107]", got)
	}
}

func TestCursorAReplicaLaggingLongerThanAPassAdvancesBehindTheLag(t *testing.T) {
	conn, db, cluster, replica := openReplicatedCluster(t)
	store := ClusterStore{Conn: conn, Cluster: cluster, Database: db}
	spans := fmt.Sprintf("FUNCTION remote('%s', %s, tempo_traces)", replica, db)
	t.Cleanup(func() { onLocalReplica(t, conn, db, "START FETCHES") })
	c := serverNow(t, conn).Add(-time.Hour)
	var reads Reads
	var starts []time.Time
	for i := range 4 {
		onLocalReplica(t, conn, db, "START FETCHES")
		onLocalReplica(t, conn, db, "SYNC REPLICA")
		onLocalReplica(t, conn, db, "STOP FETCHES")
		insert(t, conn, chain(10+i, 2)...)
		starts = append(starts, serverNow(t, conn))
		insertInto(t, conn, spans, chain(100+i, 2)...)
		time.Sleep(3 * time.Second)
		next, read, err := store.Pass(context.Background(), time.Millisecond, 1000, c, reads)
		if err != nil {
			t.Fatal(err)
		}
		if next.After(starts[i]) {
			t.Errorf("pass %d: cursor %v passed the start %v of an insert this replica has yet to fetch", i, next, starts[i])
		}
		c, reads = next, reads.add(read)
	}
	if !c.After(starts[1]) {
		t.Errorf("cursor %v, want it to advance behind the lag, past %v", c, starts[1])
	}
	if got := clusterNumbered(t, conn, cluster, db, 10, 11); fmt.Sprint(got) != "[10 11]" {
		t.Errorf("numbered %v while this replica lagged, want [10 11]", got)
	}
}

func TestCursorAReplicaLaggingLongerThanEightPassesAdvancesBehindTheLag(t *testing.T) {
	conn, db, cluster, replica := openReplicatedCluster(t)
	const idle = 8 * time.Second
	store := ClusterStore{Conn: conn, Cluster: cluster, Database: db}
	insert(t, conn, chain(10, 2)...)
	touched := serverNow(t, conn)
	syncReplicas(t, conn, cluster, db)
	onLocalReplica(t, conn, db, "STOP FETCHES")
	t.Cleanup(func() { onLocalReplica(t, conn, db, "START FETCHES") })
	first, read, err := store.Pass(context.Background(), idle, 1000, touched.Add(-time.Hour), Reads{})
	if err != nil {
		t.Fatal(err)
	}
	reads := Reads{}.add(read)
	time.Sleep(4500 * time.Millisecond)
	start := serverNow(t, conn)
	insertInto(t, conn, fmt.Sprintf("FUNCTION remote('%s', %s, tempo_traces)", replica, db), chain(100, 2)...)
	// Only the first read precedes the unfetched insert; trace 10 turns idle before the tenth pass.
	c := first
	for i := range 10 {
		if i == 9 {
			time.Sleep(touched.Add(idle + 200*time.Millisecond).Sub(serverNow(t, conn)))
		}
		next, read, err := store.Pass(context.Background(), idle, 1000, c, reads)
		if err != nil {
			t.Fatal(err)
		}
		if next.After(start) {
			t.Errorf("pass %d: cursor %v passed the start %v of an insert this replica has yet to fetch", i, next, start)
		}
		c, reads = next, reads.add(read)
	}
	if !c.After(first) {
		t.Errorf("cursor %v, want it to advance behind the lag, past %v", c, first)
	}
	if got := clusterNumbered(t, conn, cluster, db, 10, 100); fmt.Sprint(got) != "[10]" {
		t.Errorf("numbered %v after ten passes behind the lag, want [10]", got)
	}
}

func TestCursorRunningInsertsMatchEveryFormOfTheDatabasesName(t *testing.T) {
	conn := open(t)
	for _, c := range []struct {
		db, current, query string
		want               bool
	}{
		{"my.db", "default", "INSERT INTO `my.db`.tempo_traces (a) VALUES", true},
		{"my.db", "default", `INSERT INTO "my.db".tempo_traces_dist(a) VALUES`, true},
		{"my.db", "default", "insert into `my.db` . `tempo_traces` SELECT 1", true},
		{"my db", "default", "INSERT INTO `my db`.tempo_traces\nVALUES", true},
		{"my.db", "default", "INSERT INTO `other.db`.tempo_traces VALUES", false},
		{"my.db", "default", "INSERT INTO `my``db`.tempo_traces VALUES", true},
		{"my.db", "default", "INSERT INTO `my.db`.tempo_traces_nested_set_batch VALUES", false},
		{"my.db", "default", "INSERT INTO other.t SELECT * FROM `my.db`.tempo_traces", false},
		{"plain", "default", "INSERT INTO TABLE plain . tempo_traces VALUES", true},
		{"plain", "plain", "INSERT INTO tempo_traces VALUES", true},
		{"plain", "other", "INSERT INTO tempo_traces VALUES", false},
		{"plain", "default", "INSERT INTO other.tempo_traces VALUES", false},
	} {
		var n uint64
		q := "SELECT count() FROM (SELECT 'Insert' AS query_kind, '' AS initial_query_id, " + quote(c.query) +
			" AS query, " + quote(c.current) + " AS current_database) WHERE " + tables{db: c.db}.runningInserts()
		if err := conn.QueryRow(context.Background(), q).Scan(&n); err != nil {
			t.Fatalf("%v\n%s", err, q)
		}
		if (n == 1) != c.want {
			t.Errorf("%q into %q from %q: matched %v, want %v", c.query, c.db, c.current, n == 1, c.want)
		}
	}
}

func TestCursorTheNumberingFailsWithoutAFrontierRowOverAnEmptyTempoTraces(t *testing.T) {
	conn := open(t)
	st := localStatements(time.Millisecond, 1000, replica{delay: "toUInt64(0)", since: "NULL"})
	if err := conn.Exec(context.Background(), st.number("00000000-0000-0000-0000-000000000003")); err == nil || !strings.Contains(err.Error(), "no frontier row") {
		t.Errorf("numbering over an empty tempo_traces without a frontier row: err %v, want the missing frontier row", err)
	}
}
