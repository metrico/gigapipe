package maintenance

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/qryn/v5/ctrl/logger"
	"github.com/metrico/qryn/v5/ctrl/qryn/sql"
	"github.com/metrico/qryn/v5/shared/chtest"
)

func showCreate(t *testing.T, conn clickhouse.Conn, table string) string {
	t.Helper()
	var create string
	if err := conn.QueryRow(context.Background(), "SHOW CREATE TABLE "+table).Scan(&create); err != nil {
		t.Fatalf("%s: %v", table, err)
	}
	return create
}

func assertNestedSetSchema(t *testing.T, conn clickhouse.Conn) {
	t.Helper()
	ns := showCreate(t, conn, "tempo_traces_nested_set")
	for _, want := range []string{"ReplacingMergeTree(version)", "PARTITION BY date", "ORDER BY (trace_id, span_id)"} {
		if !strings.Contains(ns, want) {
			t.Errorf("tempo_traces_nested_set lacks %q:\n%s", want, ns)
		}
	}
	touched := showCreate(t, conn, "tempo_traces_touched")
	for _, want := range []string{"ReplacingMergeTree(inserted_at)", "ORDER BY trace_id", "TTL"} {
		if !strings.Contains(touched, want) {
			t.Errorf("tempo_traces_touched lacks %q:\n%s", want, touched)
		}
	}
	ctx := context.Background()
	if err := conn.Exec(ctx, `INSERT INTO tempo_traces (oid, trace_id, span_id, parent_id, timestamp_ns) VALUES
		('0', unhex('0102030405060708090a0b0c0d0e0f10'), unhex('0102030405060708'), '', 1),
		('0', unhex('0102030405060708090a0b0c0d0e0f10'), unhex('0102030405060709'), '', 2)`); err != nil {
		t.Fatal(err)
	}
	var n uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM tempo_traces_touched
		WHERE trace_id = unhex('0102030405060708090a0b0c0d0e0f10') AND inserted_at > now64(3) - INTERVAL 1 MINUTE`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("one insert of two spans of one trace: %d touched rows, want 1", n)
	}
	if batch := showCreate(t, conn, "tempo_traces_nested_set_batch"); !strings.Contains(batch, "ENGINE = MergeTree") ||
		!strings.Contains(batch, "ORDER BY (pass, trace_id)") || !strings.Contains(batch, "TTL") {
		t.Errorf("tempo_traces_nested_set_batch is not a local MergeTree by (pass, trace_id) with a TTL:\n%s", batch)
	}
	assertLeaseSchema(t, conn, "ENGINE = MergeTree")
	if err := conn.Exec(ctx, `INSERT INTO tempo_traces_nested_set (trace_id, span_id, date, left, right, parent, version) VALUES
		(unhex('0102030405060708090a0b0c0d0e0f10'), unhex('0102030405060708'), today(), 1, 2, -1, toUInt64(toUnixTimestamp64Milli(now64(3)))),
		(unhex('0102030405060708090a0b0c0d0e0f10'), unhex('0102030405060709'), today(), 1, 2, -1, toUInt64(toUnixTimestamp64Milli(now64(3))) + 2)`); err != nil {
		t.Fatal(err)
	}
	var newest bool
	if err := conn.QueryRow(ctx, `SELECT (SELECT max(version) FROM tempo_traces_numbered
		WHERE trace_id = unhex('0102030405060708090a0b0c0d0e0f10')) = (SELECT max(version) FROM tempo_traces_nested_set)`).Scan(&newest); err != nil {
		t.Fatal(err)
	}
	if !newest {
		t.Error("the newest numbering version is not the trace's newest nested-set version")
	}
}

func assertLeaseSchema(t *testing.T, conn clickhouse.Conn, engine string) {
	t.Helper()
	lease := showCreate(t, conn, "tempo_traces_nested_set_lease")
	for _, want := range []string{engine, "ORDER BY term", "TTL toDateTime(expires_at) + toIntervalDay(1)",
		"non_replicated_deduplication_window = 100"} {
		if !strings.Contains(lease, want) {
			t.Errorf("tempo_traces_nested_set_lease lacks %q:\n%s", want, lease)
		}
	}
	if strings.Contains(lease, "PARTITION BY") {
		t.Errorf("tempo_traces_nested_set_lease is partitioned:\n%s", lease)
	}
}

func TestNestedSetLeaseTakesTheStoragePolicy(t *testing.T) {
	conn, db := chtest.Open(t)
	if err := Update(conn, db, "", CLUST_MODE_SINGLE, 7, "default", "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	if lease := showCreate(t, conn, "tempo_traces_nested_set_lease"); !strings.Contains(lease,
		"non_replicated_deduplication_window = 100, storage_policy = 'default'") {
		t.Errorf("tempo_traces_nested_set_lease lacks the storage policy:\n%s", lease)
	}
}

func TestNestedSetSchemaOnFreshInstall(t *testing.T) {
	conn, db := chtest.Open(t)
	if err := Update(conn, db, "", CLUST_MODE_SINGLE, 7, "", "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	assertNestedSetSchema(t, conn)
}

func TestNestedSetSchemaOnUpgrade(t *testing.T) {
	conn, db := chtest.Open(t)
	stmts, err := getSQLFile(sql.TracesScript)
	if err != nil {
		t.Fatal(err)
	}
	if err := updateScripts(conn, db, "", 1, sql.LogScript, false, 7, "", "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	// The schema before the first nested-set statement.
	first := slices.IndexFunc(stmts, func(s string) bool { return strings.Contains(s, "tempo_traces_nested_set") })
	previous := strings.Join(stmts[:first], ";\n\n")
	if err := updateScripts(conn, db, "", 2, previous, false, 7, "", "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	if exists, err := tableExists(conn, "tempo_traces_nested_set"); err != nil || exists {
		t.Fatalf("previous schema has tempo_traces_nested_set=%v (err %v)", exists, err)
	}
	if err := Update(conn, db, "", CLUST_MODE_SINGLE, 7, "", "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	assertNestedSetSchema(t, conn)
}

func TestNestedSetRetentionFollowsTraces(t *testing.T) {
	conn, db := chtest.Open(t)
	if err := Update(conn, db, "", CLUST_MODE_SINGLE, 7, "", "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	if err := Rotate(conn, "", false, nil, 7, 7, "", logger.Logger); err != nil {
		t.Fatal(err)
	}
	if ns := showCreate(t, conn, "tempo_traces_nested_set"); !strings.Contains(ns, "TTL date + toIntervalDay(7)") {
		t.Errorf("tempo_traces_nested_set has no 7-day retention:\n%s", ns)
	}
}
