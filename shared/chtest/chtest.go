// Package chtest gives Go tests a fresh database on a live ClickHouse.
package chtest

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// AddrEnv names the native-protocol address (host:port) of the ClickHouse the
// tests use. Tests that need it skip when it is unset.
const AddrEnv = "GIGAPIPE_TEST_CLICKHOUSE"

// ClusterEnv names a cluster of the AddrEnv server, with at least two shards
// and no replicas. Tests that need it skip when it is unset.
const ClusterEnv = "GIGAPIPE_TEST_CLICKHOUSE_CLUSTER"

// ReplicatedClusterEnv names a cluster of the AddrEnv server whose shards
// have two replicas each, with Keeper. Tests that need it skip when it is unset.
const ReplicatedClusterEnv = "GIGAPIPE_TEST_CLICKHOUSE_REPLICATED_CLUSTER"

// Open creates an empty database and returns a connection that uses it by
// default. The database is dropped when the test ends.
func Open(t *testing.T) (clickhouse.Conn, string) {
	t.Helper()
	return open(t, "")
}

// OpenCluster is Open with the database created on every node of the
// ClusterEnv cluster, whose name it also returns.
func OpenCluster(t *testing.T) (clickhouse.Conn, string, string) {
	t.Helper()
	return openCluster(t, ClusterEnv)
}

// OpenReplicatedCluster is OpenCluster over the ReplicatedClusterEnv cluster.
func OpenReplicatedCluster(t *testing.T) (clickhouse.Conn, string, string) {
	t.Helper()
	return openCluster(t, ReplicatedClusterEnv)
}

func openCluster(t *testing.T, env string) (clickhouse.Conn, string, string) {
	t.Helper()
	cluster := os.Getenv(env)
	if cluster == "" {
		t.Skipf("%s is not set", env)
	}
	conn, db := open(t, cluster)
	return conn, db, cluster
}

func open(t *testing.T, cluster string) (clickhouse.Conn, string) {
	t.Helper()
	addr := os.Getenv(AddrEnv)
	if addr == "" {
		t.Skipf("%s is not set", AddrEnv)
	}
	onCluster := ""
	if cluster != "" {
		onCluster = " ON CLUSTER `" + cluster + "`"
	}
	ctx := context.Background()
	admin, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}})
	if err != nil {
		t.Fatal(err)
	}
	db := fmt.Sprintf("chtest_%d", rand.Uint64())
	if err := admin.Exec(ctx, "CREATE DATABASE "+db+onCluster); err != nil {
		t.Fatal(err)
	}
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}, Auth: clickhouse.Auth{Database: db}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		_ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+db+onCluster+" SYNC")
		_ = admin.Close()
	})
	return conn, db
}
