// Package nestedset numbers each trace's spans depth-first and stores
// nestedSetLeft, nestedSetRight and nestedSetParent in tempo_traces_nested_set.
package nestedset

import (
	"context"
	_ "embed"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

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

// replicaDelay is the seconds tempo_traces on the running node lags its
// replicas; 0 when it is not replicated.
func (t tables) replicaDelay() string {
	db := "currentDatabase()"
	if t.db != "" {
		db = quote(t.db)
	}
	return "(SELECT max(absolute_delay) FROM system.replicas WHERE database = " + db + " AND table = 'tempo_traces')"
}

func quote(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

// batchSelect returns the SELECT of a pass's batch; delay is an SQL expression.
func batchSelect(t tables, id string, idle time.Duration, maxSpans int, delay string) string {
	return strings.NewReplacer(
		"{DB}", t.prefix(),
		"{PASS}", passID(id),
		"{IDLE_MS}", strconv.FormatInt(idle.Milliseconds(), 10),
		"{MAX_SPANS}", strconv.Itoa(maxSpans),
		"{REPLICA_DELAY}", delay,
	).Replace(batchSQL)
}

// numberSelect returns the SELECT that numbers a pass's batch.
func numberSelect(t tables, id string) string {
	return strings.NewReplacer("{DB}", t.prefix(), "{PASS}", passID(id)).Replace(numberSQL)
}

func passID(id string) string { return "toUUID(" + quote(id) + ")" }

// passSettings are the settings both statements of a pass need.
const passSettings = "SETTINGS enable_analyzer = 1, join_use_nulls = 0"

// clusterSettings run a pass on every shard, on the first healthy replica of
// each (the initiator's shard included) so that both statements of a pass
// land on the same one.
const clusterSettings = passSettings + ", parallel_distributed_insert_select = 2, load_balancing = 'in_order', prefer_localhost_replica = 0"

const (
	batchColumns     = "(pass, trace_id)"
	nestedSetColumns = "(trace_id, span_id, date, left, right, parent, version)"
)

// statements renders the two statements of a pass by pass id.
type statements struct{ batch, number func(id string) string }

// localStatements are the statements of a pass over the local tables.
func localStatements(idle time.Duration, maxSpans int, delay string) statements {
	t := tables{}
	return statements{
		batch: func(id string) string {
			return "INSERT INTO tempo_traces_nested_set_batch " + batchColumns + "\n" +
				batchSelect(t, id, idle, maxSpans, delay) + "\n" + passSettings
		},
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
		batch: func(id string) string {
			return onShards("tempo_traces_nested_set_batch_dist", batchColumns, batchSelect(t, id, idle, maxSpans, t.replicaDelay()))
		},
		number: func(id string) string {
			return onShards("tempo_traces_nested_set_dist", nestedSetColumns, numberSelect(t, id))
		},
	}
}

// inserter runs an INSERT and returns a count that is 0 only when no node
// inserted a row.
type inserter func(ctx context.Context, query string) (uint64, error)

// run runs one pass under a new pass id: the batch, then its numbering
// unless the batch is empty.
func run(ctx context.Context, insert inserter, st statements) error {
	id := uuid.NewString()
	n, err := insert(ctx, st.batch(id))
	if err != nil || n == 0 {
		return err
	}
	_, err = insert(ctx, st.number(id))
	return err
}

// connInserter sums the InsertedRows profile events of every node the
// statement ran on.
func connInserter(conn clickhouse.Conn) inserter {
	return func(ctx context.Context, query string) (uint64, error) {
		var n uint64
		ctx = clickhouse.Context(ctx, clickhouse.WithProfileEvents(func(events []clickhouse.ProfileEvent) {
			for _, e := range events {
				if e.Name == "InsertedRows" && e.Value > 0 {
					n += uint64(e.Value)
				}
			}
		}))
		err := conn.Exec(ctx, query)
		return n, err
	}
}

// Pass numbers the traces idle for at least idle, oldest first, up to
// maxSpans spans; a single trace over the cap is numbered alone.
func Pass(ctx context.Context, conn clickhouse.Conn, idle time.Duration, maxSpans int) error {
	return run(ctx, connInserter(conn), localStatements(idle, maxSpans, tables{}.replicaDelay()))
}

// ClusterPass is Pass on every shard of cluster over the tables of db, each
// shard numbering its own traces; maxSpans applies per shard.
func ClusterPass(ctx context.Context, conn clickhouse.Conn, cluster, db string, idle time.Duration, maxSpans int) error {
	return run(ctx, connInserter(conn), clusterStatements(cluster, db, idle, maxSpans))
}
