package cluster

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/metrico/qryn/v4/writer/utils/logger"
)

// Conn is the minimal ClickHouse interface needed for cluster coordination.
// Both chwrapper.IChClient and clickhouse.Conn satisfy it.
type Conn interface {
	Exec(ctx context.Context, query string, args ...any) error
	Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error)
}

var (
	nodeID     string
	leaderFlag atomic.Bool
	stopCh     chan struct{}
	doneCh     chan struct{}
)

// Init starts the cluster heartbeat and leader-election loop.
// conn is shared with the writer — no separate connection is created.
// isDistributed controls whether reads fan out via the _dist table.
func Init(ctx context.Context, conn Conn, dbName string, isDistributed bool) {
	id, err := uuid.NewV6()
	if err != nil {
		panic("cluster: failed to generate UUIDv6: " + err.Error())
	}
	nodeID = id.String()
	stopCh = make(chan struct{})
	doneCh = make(chan struct{})

	writeTable := dbName + ".qryn_cluster_nodes"
	readTable := writeTable
	if isDistributed {
		writeTable = dbName + ".qryn_cluster_nodes_dist"
		readTable = writeTable
	}

	logger.Info("cluster node ID: ", nodeID)

	// Establish presence and leadership state before the rest of the app starts.
	beat(ctx, conn, writeTable, readTable)

	go run(conn, writeTable, readTable)
}

// IsLeader reports whether this instance currently holds the leader role.
func IsLeader() bool { return leaderFlag.Load() }

// NodeID returns this instance's UUIDv6 string.
func NodeID() string { return nodeID }

// Stop signals the heartbeat goroutine to exit and waits for it to finish.
func Stop() {
	if stopCh == nil {
		return
	}
	close(stopCh)
	<-doneCh
}

func run(conn Conn, writeTable, readTable string) {
	defer close(doneCh)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			beat(context.Background(), conn, writeTable, readTable)
		case <-stopCh:
			retire(conn, writeTable)
			return
		}
	}
}

func beat(ctx context.Context, conn Conn, writeTable, readTable string) {
	now := time.Now().UTC()
	err := conn.Exec(ctx,
		"INSERT INTO "+writeTable+" (key, value, updated_at, ttl) VALUES (?, ?, ?, ?)",
		nodeID, "true", now, now.Add(time.Hour))
	if err != nil {
		logger.Error("cluster heartbeat insert failed: ", err)
		return
	}
	elect(ctx, conn, readTable)
}

func elect(ctx context.Context, conn Conn, readTable string) {
	rows, err := conn.Query(ctx,
		"SELECT key FROM "+readTable+
			" GROUP BY key"+
			" HAVING argMax(value, updated_at) = 'true'"+
			" AND max(updated_at) >= now() - INTERVAL 60 SECOND"+
			" ORDER BY key ASC LIMIT 1")
	if err != nil {
		logger.Error("cluster leader election query failed: ", err)
		leaderFlag.Store(false)
		return
	}
	defer rows.Close()
	if rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			logger.Error("cluster leader election scan failed: ", err)
			leaderFlag.Store(false)
			return
		}
		leaderFlag.Store(key == nodeID)
	} else {
		leaderFlag.Store(false)
	}
}

// retire marks this node as dead so peers elect a new leader within one tick.
// It inserts value='false' with a current updated_at so it wins ReplacingMergeTree
// deduplication and is filtered out of the election query immediately.
func retire(conn Conn, writeTable string) {
	now := time.Now().UTC()
	err := conn.Exec(context.Background(),
		"INSERT INTO "+writeTable+" (key, value, updated_at, ttl) VALUES (?, ?, ?, ?)",
		nodeID, "false", now, now)
	if err != nil {
		logger.Error("cluster retire insert failed: ", err)
	}
}
