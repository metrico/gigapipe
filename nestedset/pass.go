// Package nestedset numbers each trace's spans depth-first and stores
// nestedSetLeft, nestedSetRight and nestedSetParent in tempo_traces_nested_set.
package nestedset

import (
	"context"
	_ "embed"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

// zeroTrace is the trace id of each shard's frontier row in the batch table.
const zeroTrace = "toFixedString('', 16)"

// noFrontier is the error message of a statement on a node without the
// pass's frontier row.
const noFrontier = "'no frontier row on this node'"

// cursorSet reports whether c is a cursor; the zero time and the epoch are not.
func cursorSet(c time.Time) bool { return c.UnixMilli() > 0 }

// timeExpr renders a time as DateTime64(3); an unset one is the epoch.
func timeExpr(c time.Time) string {
	return "fromUnixTimestamp64Milli(toInt64(" + strconv.FormatInt(max(c.UnixMilli(), 0), 10) + "))"
}

//go:embed batch.sql
var batchSQL string

//go:embed number.sql
var numberSQL string

// tables qualifies the table names of a pass: unqualified ones resolve in
// the connection's database.
type tables struct{ db string }

func (t tables) prefix() string {
	if t.db == "" {
		return ""
	}
	return "`" + strings.NewReplacer(`\`, `\\`, "`", "\\`").Replace(t.db) + "`."
}

// database is the SQL expression of the database the tables are in.
func (t tables) database() string {
	if t.db == "" {
		return "currentDatabase()"
	}
	return quote(t.db)
}

// replicaTables are the replicated tables a pass reads spans and touches from.
const replicaTables = "table IN ('tempo_traces', 'tempo_traces_touched')"

// replicaDelay is the seconds tempo_traces or tempo_traces_touched on the
// running node lags its replicas, at least idle while it has replication log
// entries left to pull; 0 when they are not replicated.
func (t tables) replicaDelay(idle time.Duration) string {
	idleS := strconv.FormatInt(int64((idle+time.Second-1)/time.Second), 10)
	return "(SELECT max(if(log_pointer <= log_max_index, greatest(absolute_delay, " + idleS + "), absolute_delay))" +
		" FROM system.replicas WHERE database = " + t.database() + " AND " + replicaTables + ")"
}

// replicaSince is 2s before a time no later than any replication log entry the
// node has yet to apply; NULL while it has none, no queued insert and no delay.
func (t tables) replicaSince() string {
	return replicaSince("system.replicas WHERE database = " + t.database() + " AND " + replicaTables)
}

// replicaSince is tables.replicaSince over the rows of from, which has the
// columns of system.replicas it reads.
func replicaSince(from string) string {
	behind := "log_pointer <= log_max_index OR inserts_in_queue > 0 OR absolute_delay > 0"
	pulled := "if(toUnixTimestamp(last_queue_update) = 0, now() - uptime(), last_queue_update)"
	return "(SELECT minIfOrNull(toDateTime64(least(if(log_pointer <= log_max_index, " + pulled + ", now())," +
		" if(inserts_in_queue > 0, inserts_oldest_time, now()), now() - toIntervalSecond(absolute_delay)), 0)" +
		" - toIntervalSecond(2), " + behind + ") FROM " + from + ")"
}

func quote(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

// insertRE matches an INSERT into tempo_traces or tempo_traces_dist, its
// database qualifier, quoted or bare, in group 1.
const insertRE = `(?i)INSERT\\s+INTO\\s+(?:TABLE\\s+)?` +
	`(?:(\\x60(?:[^\\x60\\\\]|\\x60\\x60|\\\\.)+\\x60|"(?:[^"\\\\]|""|\\\\.)+"|[^\\s.\\x60"(]+)\\s*\\.\\s*)?` +
	`[\\x60"]?tempo_traces(?:_dist)?[\\x60"]?(?:\\s|\\(|$)`

// Database qualifiers runningInserts compares: a quoted name without escapes
// or quotes inside, and a bare name.
const (
	quotedDBRE = `^(\\x60[^\\x60\\\\]*\\x60|"[^"\\\\]*")$`
	bareDBRE   = `^[^\\x60"]+$`
)

// runningInserts matches the rows of system.processes that insert into the
// tables' tempo_traces or tempo_traces_dist, other than the query running it.
// An insert whose database qualifier is neither form counts as one.
func (t tables) runningInserts() string {
	return "query_kind = 'Insert' AND initial_query_id != initialQueryID() AND match(query, '" + insertRE + "')" +
		" AND ifNull(multiIf((regexpExtract(query, '" + insertRE + "', 1) AS insert_db) = '', current_database," +
		" match(insert_db, '" + quotedDBRE + "'), substring(insert_db, 2, length(insert_db) - 2)," +
		" match(insert_db, '" + bareDBRE + "'), insert_db, NULL) = " + t.database() + ", 1)"
}

// RunningRead is a pass's read of the oldest running insert: the time on the
// nodes after they were read, and the oldest start, or that time when none ran.
type RunningRead struct{ At, Start time.Time }

// readBuckets is how many power-of-two age buckets Reads keeps besides the newest read.
const readBuckets = 12

// Reads are an instance's running reads, oldest first: the newest, and for each
// k < readBuckets the oldest kept read at most 2^k reads older than it.
type Reads struct {
	n    int // reads added
	kept []keptRead
}

type keptRead struct {
	RunningRead
	n int // its position among the reads added
}

// add returns r with read added; a read without a time is not added.
func (r Reads) add(read RunningRead) Reads {
	if !cursorSet(read.At) {
		return r
	}
	next := Reads{n: r.n + 1}
	for k := readBuckets - 1; k >= 0; k-- {
		for _, old := range r.kept {
			if next.n-old.n <= 1<<k {
				if len(next.kept) == 0 || next.kept[len(next.kept)-1].n < old.n {
					next.kept = append(next.kept, old)
				}
				break
			}
		}
	}
	next.kept = append(next.kept, keptRead{read, next.n})
	return next
}

// all returns r's reads, oldest first.
func (r Reads) all() []RunningRead {
	s := make([]RunningRead, len(r.kept))
	for i, k := range r.kept {
		s[i] = k.RunningRead
	}
	return s
}

// expr renders r as an Array(Tuple(at, start)).
func (r Reads) expr() string {
	s := make([]string, len(r.kept))
	for i, read := range r.all() {
		s[i] = "(" + timeExpr(read.At) + ", " + timeExpr(read.Start) + ")"
	}
	return "CAST([" + strings.Join(s, ", ") + "], 'Array(Tuple(DateTime64(3), DateTime64(3)))')"
}

// runningSelect returns the oldest start of a running insert on the node, or
// its time when there is none, and its time.
func runningSelect(t tables) string {
	return "SELECT ifNull(minOrNull(now64(3) - toIntervalMillisecond(toInt64(ceil(elapsed * 1000)))), now64(3)) AS start,\n" +
		"       now64(3) AS at\n" +
		"FROM system.processes WHERE " + t.runningInserts()
}

// replica renders the replication state a pass's batch reads.
type replica struct{ delay, since string }

// batchSelect returns the SELECT of a pass's batch from cursor, cut off at
// running and, while the node has replication log entries to apply, at reads.
func batchSelect(t tables, id string, idle time.Duration, maxSpans int, r replica, cursor time.Time, running RunningRead, reads Reads) string {
	return strings.NewReplacer(
		"{CURSOR}", timeExpr(cursor),
		"{RUNNING}", timeExpr(running.Start),
		"{READS}", reads.expr(),
		"{RUNNING_INSERTS}", t.runningInserts(),
		"{ZERO}", zeroTrace,
		"{DB}", t.prefix(),
		"{PASS}", passID(id),
		"{IDLE_MS}", strconv.FormatInt(idle.Milliseconds(), 10),
		"{MAX_SPANS}", strconv.Itoa(maxSpans),
		"{REPLICA_DELAY}", r.delay,
		"{REPLICA_SINCE}", r.since,
	).Replace(batchSQL)
}

// numberSelect returns the SELECT that numbers a pass's batch.
func numberSelect(t tables, id string) string {
	return strings.NewReplacer("{DB}", t.prefix(), "{PASS}", passID(id), "{ZERO}", zeroTrace, "{NO_FRONTIER}", noFrontier).Replace(numberSQL)
}

func passID(id string) string { return "toUUID(" + quote(id) + ")" }

// passSettings are the settings both statements of a pass need.
const passSettings = "SETTINGS enable_analyzer = 1, join_use_nulls = 0"

// runningReadSettings skip a replica that does not connect within 1s or
// complete the handshake within 2s, and fail the read on one that then takes
// over 3s to answer.
const runningReadSettings = "skip_unavailable_shards = 1, connections_with_failover_max_tries = 1," +
	" connect_timeout_with_failover_ms = 1000, handshake_timeout_ms = 2000, send_timeout = 3, receive_timeout = 3"

// clusterSettings run a pass on every shard, on the first healthy replica of
// each (the initiator's shard included) so that both statements of a pass
// land on the same one.
const clusterSettings = passSettings + ", parallel_distributed_insert_select = 2, load_balancing = 'in_order', prefer_localhost_replica = 0"

const (
	batchColumns     = "(pass, trace_id, touched)"
	nestedSetColumns = "(trace_id, span_id, date, left, right, parent, version)"
)

// statements renders the read of the oldest running insert, the two
// statements of a pass by pass id, and the read of its batch size and frontier.
type statements struct {
	running  string
	batch    func(id string, cursor time.Time, running RunningRead, reads Reads) string
	frontier func(id string) string
	number   func(id string) string
}

// frontierSelect returns a node's batch size without its frontier row, and
// its frontier; it fails when the frontier row is missing.
func frontierSelect(t tables, id string) string {
	return "SELECT countIf(trace_id != " + zeroTrace + ") AS n,\n" +
		"       maxIf(touched, trace_id = " + zeroTrace + ") +\n" +
		"         toIntervalMillisecond(throwIf(countIf(trace_id = " + zeroTrace + ") = 0, " + noFrontier + ")) AS f\n" +
		"FROM " + t.prefix() + "tempo_traces_nested_set_batch WHERE pass = " + passID(id)
}

// localStatements are the statements of a pass over the local tables.
func localStatements(idle time.Duration, maxSpans int, r replica) statements {
	t := tables{}
	return statements{
		running: runningSelect(t),
		batch: func(id string, cursor time.Time, running RunningRead, reads Reads) string {
			return "INSERT INTO tempo_traces_nested_set_batch " + batchColumns + "\n" +
				batchSelect(t, id, idle, maxSpans, r, cursor, running, reads) + "\n" + passSettings
		},
		frontier: func(id string) string { return frontierSelect(t, id) },
		number: func(id string) string {
			return "INSERT INTO tempo_traces_nested_set " + nestedSetColumns + "\n" + numberSelect(t, id) + "\n" + passSettings
		},
	}
}

// clusterStatements are the statements of a pass on every shard of cluster
// over the tables of db, each shard inserting into its own local tables.
func clusterStatements(cluster, db string, idle time.Duration, maxSpans int) statements {
	t := tables{db: db}
	onShards := func(table, columns, sel string) string {
		return "INSERT INTO " + t.prefix() + table + " " + columns + "\n" +
			"SELECT * FROM cluster(" + quote(cluster) + ", view(\n" + sel + "\n))\n" + clusterSettings
	}
	return statements{
		running: "SELECT min(start), max(at) FROM clusterAllReplicas(" + quote(cluster) + ", view(\n" + runningSelect(t) + "\n))\n" +
			"SETTINGS " + runningReadSettings,
		batch: func(id string, cursor time.Time, running RunningRead, reads Reads) string {
			r := replica{delay: t.replicaDelay(idle), since: t.replicaSince()}
			return onShards("tempo_traces_nested_set_batch_dist", batchColumns, batchSelect(t, id, idle, maxSpans, r, cursor, running, reads))
		},
		frontier: func(id string) string {
			return "SELECT sum(n), min(f) FROM cluster(" + quote(cluster) + ", view(\n" + frontierSelect(t, id) + "\n))\n" +
				"SETTINGS load_balancing = 'in_order', prefer_localhost_replica = 0"
		},
		number: func(id string) string {
			return onShards("tempo_traces_nested_set_dist", nestedSetColumns, numberSelect(t, id))
		},
	}
}

// inserter runs an INSERT.
type inserter func(ctx context.Context, query string) error

// reader runs a query of one row and scans it into dest.
type reader func(ctx context.Context, query string, dest ...any) error

var errNoRunning = errors.New("the running-insert read reached no node")

var errNoFrontier = errors.New("the numbering pass read no frontier")

// run runs one pass from cursor under a new pass id: the read of the oldest
// running insert, the batch cut off at it and at reads, then its numbering
// unless the batch is empty. It returns the frontier the batch read, or cursor
// when any step fails, and the pass's read, zero when it took none.
func run(ctx context.Context, insert inserter, read reader, st statements, cursor time.Time, reads Reads) (time.Time, RunningRead, error) {
	id := uuid.NewString()
	var running RunningRead
	if err := read(ctx, st.running, &running.Start, &running.At); err != nil {
		return cursor, RunningRead{}, err
	}
	if !cursorSet(running.Start) || !cursorSet(running.At) {
		return cursor, RunningRead{}, errNoRunning
	}
	if err := insert(ctx, st.batch(id, cursor, running, reads)); err != nil {
		return cursor, running, err
	}
	var n uint64
	var frontier time.Time
	if err := read(ctx, st.frontier(id), &n, &frontier); err != nil {
		return cursor, running, err
	}
	if !cursorSet(frontier) {
		return cursor, running, errNoFrontier
	}
	if n > 0 {
		if err := insert(ctx, st.number(id)); err != nil {
			return cursor, running, err
		}
	}
	return frontier, running, nil
}

func connInserter(conn clickhouse.Conn) inserter {
	return func(ctx context.Context, query string) error { return conn.Exec(ctx, query) }
}

func connReader(conn clickhouse.Conn) reader {
	return func(ctx context.Context, query string, dest ...any) error {
		return conn.QueryRow(ctx, query).Scan(dest...)
	}
}

// Pass numbers the traces touched since cursor, never before now - 1 day (in
// the last 5 minutes without a cursor), and idle for at least idle, oldest
// first, up to maxSpans spans; a single trace over the cap is numbered alone.
// reads are the earlier passes' reads. It returns the cursor for the next pass
// and this pass's read.
func Pass(ctx context.Context, conn clickhouse.Conn, idle time.Duration, maxSpans int, cursor time.Time, reads Reads) (time.Time, RunningRead, error) {
	t := tables{}
	st := localStatements(idle, maxSpans, replica{delay: t.replicaDelay(idle), since: t.replicaSince()})
	return run(ctx, connInserter(conn), connReader(conn), st, cursor, reads)
}

// ClusterPass is Pass on every shard of cluster over the tables of db, each
// shard numbering its own traces; maxSpans applies per shard. The next cursor
// is the least of the shards' frontiers.
func ClusterPass(ctx context.Context, conn clickhouse.Conn, cluster, db string, idle time.Duration, maxSpans int, cursor time.Time, reads Reads) (time.Time, RunningRead, error) {
	return run(ctx, connInserter(conn), connReader(conn), clusterStatements(cluster, db, idle, maxSpans), cursor, reads)
}
