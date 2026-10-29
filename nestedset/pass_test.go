package nestedset

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/qryn/v5/ctrl/logger"
	"github.com/metrico/qryn/v5/ctrl/qryn/maintenance"
	"github.com/metrico/qryn/v5/shared/chtest"
)

// span is a seeded span: ids are small integers, parent 0 is a root.
type span struct {
	trace, id, parent int
	start             int64
}

type numbers struct{ left, right, parent int32 }

func open(t *testing.T) clickhouse.Conn {
	t.Helper()
	conn, db := chtest.Open(t)
	if err := maintenance.Update(conn, db, "", maintenance.CLUST_MODE_SINGLE, 7, "", "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	return conn
}

func traceID(n int) string { return fmt.Sprintf("unhex('%032x')", n) }
func spanID(n int) string  { return fmt.Sprintf("unhex('%016x')", n) }

func exec(t *testing.T, conn clickhouse.Conn, q string) {
	t.Helper()
	if err := conn.Exec(context.Background(), q); err != nil {
		t.Fatalf("%v\n%s", err, q)
	}
}

// insert writes spans in one insert block; start is in seconds after 2026-10-01.
func insert(t *testing.T, conn clickhouse.Conn, spans ...span) {
	t.Helper()
	insertInto(t, conn, "tempo_traces", spans...)
}

// insertInto is insert into the named table.
func insertInto(t *testing.T, conn clickhouse.Conn, table string, spans ...span) {
	t.Helper()
	rows := make([]string, len(spans))
	for i, s := range spans {
		parent := "''"
		if s.parent != 0 {
			parent = spanID(s.parent)
		}
		rows[i] = fmt.Sprintf("('0', %s, %s, %s, %d)", traceID(s.trace), spanID(s.id), parent, (1790812800+s.start)*1e9)
	}
	exec(t, conn, "INSERT INTO "+table+" (oid, trace_id, span_id, parent_id, timestamp_ns) VALUES "+strings.Join(rows, ", "))
}

// touch replaces the touched times: trace -> age at the time of the call.
func touch(t *testing.T, conn clickhouse.Conn, ages map[int]time.Duration) {
	t.Helper()
	exec(t, conn, "TRUNCATE TABLE tempo_traces_touched")
	var rows []string
	for trace, age := range ages {
		rows = append(rows, fmt.Sprintf("(%s, now64(3) - toIntervalMillisecond(%d))", traceID(trace), age.Milliseconds()))
	}
	exec(t, conn, "INSERT INTO tempo_traces_touched (trace_id, inserted_at) VALUES "+strings.Join(rows, ", "))
}

// stored returns the newest numbers and version of every numbered span of a trace.
func stored(t *testing.T, conn clickhouse.Conn, trace int) (map[int]numbers, uint64) {
	t.Helper()
	rows, err := conn.Query(context.Background(), fmt.Sprintf(`
		SELECT reinterpretAsUInt64(reverse(span_id)), argMax(left, version), argMax(right, version), argMax(parent, version), max(version)
		FROM tempo_traces_nested_set WHERE trace_id = %s GROUP BY span_id`, traceID(trace)))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[int]numbers{}
	var version uint64
	for rows.Next() {
		var id, v uint64
		var n numbers
		if err := rows.Scan(&id, &n.left, &n.right, &n.parent, &v); err != nil {
			t.Fatal(err)
		}
		got[int(id)] = n
		version = max(version, v)
	}
	return got, version
}

func pass(t *testing.T, conn clickhouse.Conn, idle time.Duration, maxSpans int) {
	t.Helper()
	if _, _, err := Pass(context.Background(), conn, idle, maxSpans, time.Time{}, Reads{}); err != nil {
		t.Fatal(err)
	}
}

func assertNumbers(t *testing.T, conn clickhouse.Conn, trace int, want map[int]numbers) uint64 {
	t.Helper()
	got, version := stored(t, conn, trace)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("trace %d:\n got %v\nwant %v", trace, got, want)
	}
	return version
}

func TestPassNumbersDepthFirst(t *testing.T) {
	conn := open(t)
	insert(t, conn,
		// Two roots ordered by start; under root 1, children 3 and 4 tie on
		// start and order by span id, child 5 starts first.
		span{1, 2, 0, 50}, span{1, 1, 0, 10},
		span{1, 4, 1, 30}, span{1, 3, 1, 30}, span{1, 5, 1, 20}, span{1, 6, 3, 40},
		span{1, 7, 2, 60},
		// Unreachable: an orphan subtree, a cycle with a child, a self-parent.
		span{1, 8, 99, 11}, span{1, 9, 8, 12},
		span{1, 10, 11, 13}, span{1, 11, 10, 14}, span{1, 12, 11, 15},
		span{1, 13, 13, 16},
		// A duplicate id: placed by its earliest copy, a root before span 1.
		span{1, 14, 7, 70}, span{1, 14, 0, 5}, span{1, 15, 14, 80},
	)
	insert(t, conn,
		// Root-less.
		span{2, 1, 99, 10}, span{2, 2, 1, 20},
	)
	touch(t, conn, map[int]time.Duration{1: time.Minute, 2: time.Minute})
	pass(t, conn, time.Second, 1000)
	assertNumbers(t, conn, 1, map[int]numbers{
		14: {1, 4, -1}, 15: {2, 3, 1},
		1: {5, 14, -1}, 5: {6, 7, 5}, 3: {8, 11, 5}, 6: {9, 10, 8}, 4: {12, 13, 5},
		2: {15, 18, -1}, 7: {16, 17, 15},
		8: {}, 9: {}, 10: {}, 11: {}, 12: {}, 13: {},
	})
	assertNumbers(t, conn, 2, map[int]numbers{1: {}, 2: {}})
}

// chain returns a trace of n spans, each the parent of the next.
func chain(trace, n int) []span {
	spans := make([]span, n)
	for i := range spans {
		spans[i] = span{trace, i + 1, i, int64(i)}
	}
	return spans
}

func numberedTraces(t *testing.T, conn clickhouse.Conn, traces ...int) []int {
	t.Helper()
	var got []int
	for _, trace := range traces {
		if n, _ := stored(t, conn, trace); len(n) > 0 {
			got = append(got, trace)
		}
	}
	return got
}

func TestPassNumbersOnlyIdleTraces(t *testing.T) {
	conn := open(t)
	insert(t, conn, chain(1, 2)...)
	insert(t, conn, chain(2, 2)...)
	touch(t, conn, map[int]time.Duration{1: time.Minute, 2: time.Second})
	pass(t, conn, 10*time.Second, 1000)
	if got := numberedTraces(t, conn, 1, 2); fmt.Sprint(got) != "[1]" {
		t.Errorf("numbered %v, want [1]", got)
	}
}

func TestPassSkipsANumberedUntouchedTrace(t *testing.T) {
	conn := open(t)
	insert(t, conn, chain(1, 2)...)
	touch(t, conn, map[int]time.Duration{1: time.Minute})
	pass(t, conn, time.Second, 1000)
	_, first := stored(t, conn, 1)
	time.Sleep(5 * time.Millisecond)
	pass(t, conn, time.Second, 1000)
	var rows uint64
	if err := conn.QueryRow(context.Background(), "SELECT count() FROM tempo_traces_nested_set").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if _, second := stored(t, conn, 1); first == 0 || second != first || rows != 2 {
		t.Errorf("versions %d then %d, %d rows; want one numbering of 2 rows", first, second, rows)
	}
}

func TestPassTakesTheOldestTracesUpToTheSpanCap(t *testing.T) {
	conn := open(t)
	for trace := 1; trace <= 3; trace++ {
		insert(t, conn, chain(trace, 3)...)
	}
	touch(t, conn, map[int]time.Duration{2: 3 * time.Minute, 3: 2 * time.Minute, 1: time.Minute})
	pass(t, conn, time.Second, 7)
	if got := numberedTraces(t, conn, 1, 2, 3); fmt.Sprint(got) != "[2 3]" {
		t.Errorf("first pass numbered %v, want [2 3]", got)
	}
	pass(t, conn, time.Second, 7)
	if got := numberedTraces(t, conn, 1, 2, 3); fmt.Sprint(got) != "[1 2 3]" {
		t.Errorf("second pass numbered %v, want [1 2 3]", got)
	}
}

func TestPassNumbersATraceOverTheCapAlone(t *testing.T) {
	conn := open(t)
	insert(t, conn, chain(1, 5)...)
	insert(t, conn, chain(2, 1)...)
	touch(t, conn, map[int]time.Duration{1: 2 * time.Minute, 2: time.Minute})
	pass(t, conn, time.Second, 2)
	if got := numberedTraces(t, conn, 1, 2); fmt.Sprint(got) != "[1]" {
		t.Errorf("numbered %v, want [1]", got)
	}
	assertNumbers(t, conn, 1, map[int]numbers{1: {1, 10, -1}, 2: {2, 9, 1}, 3: {3, 8, 2}, 4: {4, 7, 3}, 5: {5, 6, 4}})
}

func TestPassRenumbersATraceAfterALateSpan(t *testing.T) {
	conn := open(t)
	insert(t, conn, span{1, 1, 0, 10}, span{1, 2, 1, 20})
	time.Sleep(20 * time.Millisecond)
	pass(t, conn, 10*time.Millisecond, 1000)
	first := assertNumbers(t, conn, 1, map[int]numbers{1: {1, 4, -1}, 2: {2, 3, 1}})
	// The late span starts first among the root's children.
	insert(t, conn, span{1, 3, 1, 15})
	time.Sleep(20 * time.Millisecond)
	pass(t, conn, 10*time.Millisecond, 1000)
	second := assertNumbers(t, conn, 1, map[int]numbers{1: {1, 6, -1}, 3: {2, 3, 1}, 2: {4, 5, 1}})
	if second <= first {
		t.Errorf("renumbered with version %d, not newer than %d", second, first)
	}
}

func TestPassSkipsTracesTouchedWithinTheReplicaDelayPlusTwoSeconds(t *testing.T) {
	conn := open(t)
	for trace := 1; trace <= 3; trace++ {
		insert(t, conn, chain(trace, 2)...)
	}
	// A replica 5s behind: traces touched within the last 7s wait.
	touch(t, conn, map[int]time.Duration{1: 6 * time.Second, 2: 8 * time.Second, 3: 2 * time.Second})
	if _, _, err := run(context.Background(), connInserter(conn), connReader(conn), localStatements(time.Second, 1000, replica{delay: "toUInt64(5)", since: "NULL"}), time.Time{}, Reads{}); err != nil {
		t.Fatal(err)
	}
	if got := numberedTraces(t, conn, 1, 2, 3); fmt.Sprint(got) != "[2]" {
		t.Errorf("numbered %v, want [2]", got)
	}
}

func TestPassIgnoresATouchPastItsDayOfRetention(t *testing.T) {
	conn := open(t)
	insert(t, conn, chain(1, 2)...)
	insert(t, conn, chain(2, 2)...)
	// Trace 1's touch has expired but is not merged away yet.
	exec(t, conn, "ALTER TABLE tempo_traces_touched REMOVE TTL")
	touch(t, conn, map[int]time.Duration{1: 25 * time.Hour, 2: time.Minute})
	pass(t, conn, time.Second, 1000)
	if got := numberedTraces(t, conn, 1, 2); fmt.Sprint(got) != "[2]" {
		t.Errorf("numbered %v, want [2]", got)
	}
}
