package nestedset

import (
	"context"
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/qryn/v5/ctrl/logger"
	"github.com/metrico/qryn/v5/ctrl/qryn/maintenance"
	"github.com/metrico/qryn/v5/shared/chtest"
)

// hostRows returns host -> "trace/span" -> numbers of the rows a query over
// clusterAllReplicas returns; columns: host, trace, span, left, right, parent.
func hostRows(t *testing.T, conn clickhouse.Conn, q string) map[string]map[string]numbers {
	t.Helper()
	rows, err := conn.Query(context.Background(), q)
	if err != nil {
		t.Fatalf("%v\n%s", err, q)
	}
	defer rows.Close()
	got := map[string]map[string]numbers{}
	for rows.Next() {
		var host string
		var trace, id uint64
		var n numbers
		if err := rows.Scan(&host, &trace, &id, &n.left, &n.right, &n.parent); err != nil {
			t.Fatal(err)
		}
		if got[host] == nil {
			got[host] = map[string]numbers{}
		}
		got[host][fmt.Sprintf("%d/%d", trace, id)] = n
	}
	return got
}

// clusterNumbers returns, per host, the numbers each span of a 3-span chain
// should have and the newest numbers stored for it.
func clusterNumbers(t *testing.T, conn clickhouse.Conn, cluster, db string) (want, got map[string]map[string]numbers) {
	t.Helper()
	want = hostRows(t, conn, fmt.Sprintf(`
		SELECT hostName(), reinterpretAsUInt64(reverse(substring(trace_id, 9, 8))), reinterpretAsUInt64(reverse(span_id)),
		       toInt32(reinterpretAsUInt64(reverse(span_id))), toInt32(7 - reinterpretAsUInt64(reverse(span_id))),
		       if(parent_id = '', toInt32(-1), toInt32(reinterpretAsUInt64(reverse(substring(parent_id, 1, 8)))))
		FROM clusterAllReplicas('%s', %s.tempo_traces)`, cluster, db))
	got = hostRows(t, conn, fmt.Sprintf(`
		SELECT hostName(), reinterpretAsUInt64(reverse(substring(trace_id, 9, 8))), reinterpretAsUInt64(reverse(span_id)),
		       argMax(left, version), argMax(right, version), argMax(parent, version)
		FROM clusterAllReplicas('%s', %s.tempo_traces_nested_set) GROUP BY 1, 2, 3`, cluster, db))
	return want, got
}

func TestClusterPassNumbersEachShardsTracesOnThatShard(t *testing.T) {
	conn, db, cluster := chtest.OpenCluster(t)
	mode := maintenance.CLUST_MODE_SINGLE | maintenance.CLUST_MODE_DISTRIBUTED
	if err := maintenance.Update(conn, db, cluster, mode, 7, "", "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	// Chains of 3: numbers 1/6/-1, 2/5/1, 3/4/2.
	for trace := 1; trace <= 20; trace++ {
		insertInto(t, conn, "tempo_traces_dist", chain(trace, 3)...)
	}
	exec(t, conn, "SYSTEM FLUSH DISTRIBUTED tempo_traces_dist")
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{"log_comment": db}))
	if _, _, err := (ClusterStore{Conn: conn, Cluster: cluster, Database: db}).Pass(ctx, 0, 1000, time.Time{}, Reads{}); err != nil {
		t.Fatal(err)
	}

	spans, numbered := clusterNumbers(t, conn, cluster, db)
	if len(spans) < 2 {
		t.Fatalf("spans on %d hosts, want every shard", len(spans))
	}
	if !maps.EqualFunc(spans, numbered, func(a, b map[string]numbers) bool { return maps.Equal(a, b) }) {
		t.Errorf("numbered per host:\n%v\nwant:\n%v", numbered, spans)
	}

	// Every non-initiator shard runs both statements as a forwarded query; the
	// initiator may run its own part forwarded or inside the initial query.
	exec(t, conn, fmt.Sprintf("SYSTEM FLUSH LOGS ON CLUSTER '%s'", cluster))
	var initiator string
	if err := conn.QueryRow(context.Background(), "SELECT hostName()").Scan(&initiator); err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(context.Background(), fmt.Sprintf(`
		SELECT hostName() AS host, count() FROM clusterAllReplicas('%s', system.query_log)
		WHERE log_comment = '%s' AND type = 'QueryFinish' AND query_kind = 'Insert' AND NOT is_initial_query
		  AND query LIKE '%%tempo_traces_nested_set_batch%%'
		GROUP BY host`,
		cluster, db))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	forwarded := map[string]uint64{}
	for rows.Next() {
		var host string
		var n uint64
		if err := rows.Scan(&host, &n); err != nil {
			t.Fatal(err)
		}
		forwarded[host] = n
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for host := range spans {
		if host != initiator && forwarded[host] != 2 {
			t.Errorf("%s ran %d forwarded statements, want 2 (initiator %s)", host, forwarded[host], initiator)
		}
	}
}

func TestClusterPassNumbersTracesOnlyAnotherShardHas(t *testing.T) {
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
	remote := fmt.Sprintf("FUNCTION remote('%s:%d', %s, tempo_traces)", host, port, db)
	for trace := 1; trace <= 5; trace++ {
		insertInto(t, conn, remote, chain(trace, 3)...)
	}
	if _, _, err := (ClusterStore{Conn: conn, Cluster: cluster, Database: db}).Pass(context.Background(), 0, 1000, time.Time{}, Reads{}); err != nil {
		t.Fatal(err)
	}

	spans, numbered := clusterNumbers(t, conn, cluster, db)
	if len(spans) != 1 || len(spans[host]) != 15 {
		t.Fatalf("spans per host %v, want 15 on %s only", spans, host)
	}
	if !maps.EqualFunc(spans, numbered, func(a, b map[string]numbers) bool { return maps.Equal(a, b) }) {
		t.Errorf("numbered per host:\n%v\nwant:\n%v", numbered, spans)
	}
}
