//go:build integration && integration_cluster

// Cluster variant of the schema migration lane (M2, #920): with CLUSTER_NAME
// set, gigapipe's start-up migrations run ON CLUSTER and add the distributed
// tables of the *_dist.sql scripts. Every node of the cluster must end up with
// every declared table, and running the migrations again must change nothing:
// on every node, the same tables, each with the same CREATE statement.
//
// It needs a two-node cluster: `make test-integration-cluster` starts the two
// ClickHouse nodes of the e2e stack (scripts/test/e2e), with the ports from
// test/integration/cluster/docker-compose.yml, and runs this test.
//
//	CLICKHOUSE_CLUSTER        cluster name (default test_cluster_two_shards)
//	CLICKHOUSE_NODE_HTTP_URLS space-separated HTTP interfaces of the nodes
//	                          (default http://localhost:8123 http://localhost:8124)

package integration

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

var distributedScripts = []string{"log_dist.sql", "traces_dist.sql", "profiles_dist.sql", "rules_dist.sql"}

func TestSchemaMigrationsCluster(t *testing.T) {
	cluster := envOr("CLICKHOUSE_CLUSTER", "test_cluster_two_shards")
	nodes := strings.Fields(envOr("CLICKHOUSE_NODE_HTTP_URLS", "http://localhost:8123 http://localhost:8124"))
	db := fmt.Sprintf("m2_migrate_cluster_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		clickhouseQueryAt(t, nodes[0], fmt.Sprintf("DROP DATABASE IF EXISTS %s ON CLUSTER `%s` SYNC", db, cluster))
	})
	env := processEnv(db, "MODE=init_only", "CLUSTER_NAME="+cluster)

	// On a cluster the migration runner also keeps ver_dist, the distributed
	// view of ver (ctrl/qryn/maintenance/update.go).
	want := declaredTables(t, append(slices.Clone(singleNodeScripts), distributedScripts...))
	want = append(want, "ver_dist")
	slices.Sort(want)

	checkNodes := func(t *testing.T, run string) {
		t.Helper()
		for _, node := range nodes {
			if got := databaseTablesAt(t, node, db); !slices.Equal(got, want) {
				t.Errorf("tables on %s after the %s run:\n got %v\nwant %v", node, run, got, want)
			}
		}
	}
	first := map[string]map[string]string{}

	t.Run("every node gets every declared table", func(t *testing.T) {
		runGigapipe(t, env, 2*time.Minute)
		checkNodes(t, "first")
		for _, node := range nodes {
			first[node] = tableDefinitionsAt(t, node, db)
		}
	})

	t.Run("running the migrations again changes nothing", func(t *testing.T) {
		runGigapipe(t, env, 2*time.Minute)
		checkNodes(t, "second")
		for _, node := range nodes {
			assertSameDefinitions(t, node, first[node], tableDefinitionsAt(t, node, db))
		}
	})
}
