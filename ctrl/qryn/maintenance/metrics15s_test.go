package maintenance

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/ctrl/logger"
	"github.com/metrico/qryn/v5/ctrl/qryn/sql"
	"github.com/metrico/qryn/v5/shared/distconfig"
)

// The tests in this file run against a ClickHouse server given by
// CTRL_TEST_CLICKHOUSE (an HTTP URL with credentials), in databases they
// recreate, and are skipped without it. The unavailable-shard test needs the
// cluster in testdata/ctrl_dead_shard.xml.
const ctrlDeadShardCluster = "ctrl_dead_shard"

// ctrlConn recreates database db and returns a connection to it.
func ctrlConn(t *testing.T, db string) clickhouse.Conn {
	t.Helper()
	raw := os.Getenv("CTRL_TEST_CLICKHOUSE")
	if raw == "" {
		t.Skip("CTRL_TEST_CLICKHOUSE is not set")
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	pass, _ := u.User.Password()
	open := func(database string) clickhouse.Conn {
		conn, err := clickhouse.Open(&clickhouse.Options{
			Addr:     []string{u.Host},
			Protocol: clickhouse.HTTP,
			Auth:     clickhouse.Auth{Database: database, Username: u.User.Username(), Password: pass},
		})
		if err != nil {
			t.Fatal(err)
		}
		return conn
	}
	admin := open("default")
	defer admin.Close()
	for _, q := range []string{"DROP DATABASE IF EXISTS " + db, "CREATE DATABASE " + db} {
		if err := admin.Exec(context.Background(), q); err != nil {
			t.Fatal(err)
		}
	}
	conn := open(db)
	t.Cleanup(func() { conn.Close() })
	return conn
}

// ctrlSchema creates the single-node schema in db.
func ctrlSchema(t *testing.T, conn clickhouse.Conn, db string) {
	t.Helper()
	if err := Update(conn, db, "", CLUST_MODE_SINGLE, 7, "", "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
}

func ctrlExec(t *testing.T, conn clickhouse.Conn, q string) {
	t.Helper()
	if err := conn.Exec(context.Background(), q); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

// insertLines inserts one line per timestamp into samples_v3.
func insertLines(t *testing.T, conn clickhouse.Conn, ts ...int64) {
	t.Helper()
	var vals []string
	for _, v := range ts {
		vals = append(vals, fmt.Sprintf("(1, %d, 0, 'x', 1)", v))
	}
	ctrlExec(t, conn, "INSERT INTO samples_v3 (fingerprint, timestamp_ns, value, string, type) VALUES "+
		strings.Join(vals, ","))
}

// startCountMarker returns the recorded cutoff, or -1 when there is none.
func startCountMarker(t *testing.T, conn clickhouse.Conn, distributed bool) int64 {
	t.Helper()
	v, err := getSetting(conn, distributed, "update", metrics15sStartCountMarker)
	if err != nil {
		t.Fatal(err)
	}
	if v == "" {
		return -1
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return n * int64(time.Second)
}

func mvCreate(t *testing.T, conn clickhouse.Conn, db string) string {
	t.Helper()
	create, err := metrics15sMVCreate(conn, db)
	if err != nil {
		t.Fatal(err)
	}
	return create
}

// oldMV replaces the MV with one without start_count, as an older binary's
// swap leaves it.
func oldMV(t *testing.T, conn clickhouse.Conn, where string) {
	t.Helper()
	ctrlExec(t, conn, "DROP TABLE metrics_15s_mv")
	createMV(t, conn, "metrics_15s_mv", where, "")
}

// createMV creates view name feeding metrics_15s, with startCount as its
// start_count column or none.
func createMV(t *testing.T, conn clickhouse.Conn, name, where, startCount string) {
	t.Helper()
	ctrlExec(t, conn, `CREATE MATERIALIZED VIEW `+name+` TO metrics_15s AS SELECT fingerprint,
		intDiv(samples.timestamp_ns, 15000000000) * 15000000000 as timestamp_ns,
		argMaxState(value, samples.timestamp_ns) as last, maxSimpleState(value) as max,
		minSimpleState(value) as min, countState() as count, sumSimpleState(value) as sum,
		sumSimpleState(length(string)) as bytes, `+startCount+` type
		FROM samples_v3 as samples `+where+` GROUP BY fingerprint, timestamp_ns, type`)
}

// startup runs the startup upgrade on db as gigapipe does.
func startup(t *testing.T, db string) {
	t.Helper()
	u, err := url.Parse(os.Getenv("CTRL_TEST_CLICKHOUSE"))
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.ParseUint(u.Port(), 10, 32)
	if err != nil {
		t.Fatal(err)
	}
	pass, _ := u.User.Password()
	cfg := config.ClokiBaseDataBase{Host: u.Hostname(), Port: uint32(port), HttpPort: uint32(port),
		Name: db, User: u.User.Username(), Password: pass, TTLDays: 7}
	if err := upgradeDB(&cfg, logger.Logger); err != nil {
		t.Fatalf("startup: %v", err)
	}
}

// views reports which of metrics_15s_mv and metrics_15s_mv_bak exist.
func views(t *testing.T, conn clickhouse.Conn) (mv, bak bool) {
	t.Helper()
	var err error
	if mv, err = tableExists(conn, "metrics_15s_mv"); err != nil {
		t.Fatal(err)
	}
	if bak, err = tableExists(conn, "metrics_15s_mv_bak"); err != nil {
		t.Fatal(err)
	}
	return mv, bak
}

// cellCounts inserts lines at cell, cell and cell+1 and returns the cell's
// line count and start_count.
func cellCounts(t *testing.T, conn clickhouse.Conn) (count, starts uint64) {
	t.Helper()
	cell := time.Now().Add(3*time.Hour).UnixNano() / metrics15sCellNs * metrics15sCellNs
	insertLines(t, conn, cell, cell, cell+1)
	if err := conn.QueryRow(context.Background(), "SELECT countMerge(count), sum(start_count) FROM metrics_15s "+
		"WHERE timestamp_ns = $1", cell).Scan(&count, &starts); err != nil {
		t.Fatal(err)
	}
	return count, starts
}

// upgradeCutoff is the cutoff for the newest cell and clock given.
func upgradeCutoff(newest, now int64) int64 {
	return (max(newest, now)+int64(time.Minute))/metrics15sCellNs*metrics15sCellNs + metrics15sCellNs
}

func TestRecordMetrics15sStartCountFreshInstall(t *testing.T) {
	const db = "ctrl_sc_fresh"
	conn := ctrlConn(t, db)
	ctrlSchema(t, conn, db)
	if err := RecordMetrics15sStartCount(conn, db, "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	if got := startCountMarker(t, conn, false); got != 0 {
		t.Fatalf("fresh install: cutoff %d, want 0", got)
	}
	// Rows written since and a restart keep the cutoff.
	insertLines(t, conn, time.Now().UnixNano())
	if err := RecordMetrics15sStartCount(conn, db, "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	if got := startCountMarker(t, conn, false); got != 0 {
		t.Fatalf("restart: cutoff %d, want 0", got)
	}
}

func TestRecordMetrics15sStartCountUpgrade(t *testing.T) {
	const db = "ctrl_sc_upgrade"
	conn := ctrlConn(t, db)
	ctrlSchema(t, conn, db)
	// A cell ahead of the clock sets the cutoff.
	future := time.Now().Add(time.Hour).UnixNano()/metrics15sCellNs*metrics15sCellNs + 7
	insertLines(t, conn, time.Now().Add(-48*time.Hour).UnixNano(), future)
	if err := RecordMetrics15sStartCount(conn, db, "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	if got, want := startCountMarker(t, conn, false), upgradeCutoff(future/metrics15sCellNs*metrics15sCellNs, 0); got != want {
		t.Fatalf("cutoff %d, want %d", got, want)
	}

	// Older cells only: the clock sets it.
	const db2 = "ctrl_sc_upgrade_old"
	conn2 := ctrlConn(t, db2)
	ctrlSchema(t, conn2, db2)
	insertLines(t, conn2, time.Now().Add(-48*time.Hour).UnixNano())
	before := time.Now().UnixNano()
	if err := RecordMetrics15sStartCount(conn2, db2, "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	after := time.Now().UnixNano()
	got := startCountMarker(t, conn2, false)
	if got < upgradeCutoff(0, before) || got > upgradeCutoff(0, after) {
		t.Fatalf("cutoff %d outside [%d, %d]", got, upgradeCutoff(0, before), upgradeCutoff(0, after))
	}
}

func TestRecordMetrics15sStartCountSkipsWithoutColumn(t *testing.T) {
	const db = "ctrl_sc_nocolumn"
	conn := ctrlConn(t, db)
	ctrlSchema(t, conn, db)
	oldMV(t, conn, "")
	ctrlExec(t, conn, "ALTER TABLE metrics_15s DROP COLUMN start_count")
	if err := RecordMetrics15sStartCount(conn, db, "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	if got := startCountMarker(t, conn, false); got != -1 {
		t.Fatalf("cutoff %d recorded without the column", got)
	}
	if strings.Contains(mvCreate(t, conn, db), "start_count") {
		t.Fatal("MV changed without the column")
	}
}

func TestRecordMetrics15sStartCountSwapsTheMV(t *testing.T) {
	for _, where := range []string{"", metrics15sFilter} {
		t.Run(fmt.Sprintf("where=%q", where), func(t *testing.T) {
			const db = "ctrl_sc_swap"
			conn := ctrlConn(t, db)
			ctrlSchema(t, conn, db)
			if err := RecordMetrics15sStartCount(conn, db, "", false, logger.Logger); err != nil {
				t.Fatal(err)
			}
			oldMV(t, conn, where)
			before := time.Now().UnixNano()
			if err := RecordMetrics15sStartCount(conn, db, "", false, logger.Logger); err != nil {
				t.Fatal(err)
			}
			create := mvCreate(t, conn, db)
			if !strings.Contains(create, "start_count") {
				t.Fatalf("MV not swapped: %s", create)
			}
			if got := strings.Contains(create, metrics15sFilterCond); got != (where != "") {
				t.Fatalf("metric filter %v, want %v", got, where != "")
			}
			got := startCountMarker(t, conn, false)
			if got < upgradeCutoff(0, before) {
				t.Fatalf("cutoff %d before %d", got, upgradeCutoff(0, before))
			}
			// Every reader takes the largest cutoff over the marker's rows.
			var largest int64
			if err := conn.QueryRow(context.Background(), `SELECT max(toInt64OrZero(value)) FROM settings
				WHERE type = 'update' AND name = $1`, metrics15sStartCountMarker).Scan(&largest); err != nil {
				t.Fatal(err)
			}
			if largest*int64(time.Second) != got {
				t.Fatalf("largest cutoff %d, recorded %d", largest*int64(time.Second), got)
			}
			// New lines on a cell start fill start_count again.
			cell := time.Now().Add(2*time.Hour).UnixNano() / metrics15sCellNs * metrics15sCellNs
			insertLines(t, conn, cell, cell, cell+1)
			var starts uint64
			if err := conn.QueryRow(context.Background(), "SELECT sum(start_count) FROM metrics_15s WHERE timestamp_ns = $1",
				cell).Scan(&starts); err != nil {
				t.Fatal(err)
			}
			if starts != 2 {
				t.Fatalf("start_count %d, want 2", starts)
			}
		})
	}
}

// TestRecordMetrics15sStartCountDefersOnUnavailableShard: with a shard down
// the cutoff is not recorded, even through tables that skip unavailable
// shards, and the run does not fail.
func TestRecordMetrics15sStartCountDefersOnUnavailableShard(t *testing.T) {
	const db = "ctrl_dead_shard_s1"
	conn := ctrlConn(t, db)
	var shards uint64
	if err := conn.QueryRow(context.Background(), "SELECT count() FROM system.clusters WHERE cluster = $1",
		ctrlDeadShardCluster).Scan(&shards); err != nil {
		t.Fatal(err)
	}
	if shards != 2 {
		t.Skipf("no cluster %s", ctrlDeadShardCluster)
	}
	distconfig.Init()
	ctrlSchema(t, conn, db)
	for _, tbl := range []string{"settings", "metrics_15s"} {
		ctrlExec(t, conn, fmt.Sprintf("CREATE TABLE %s%s AS %[1]s ENGINE = Distributed(%[3]s, '', %[1]s) "+
			"SETTINGS skip_unavailable_shards = 1", tbl, distconfig.Suffix(), ctrlDeadShardCluster))
	}
	if err := RecordMetrics15sStartCount(conn, db, "", true, logger.Logger); err != nil {
		t.Fatal(err)
	}
	if got := startCountMarker(t, conn, false); got != -1 {
		t.Fatalf("cutoff %d recorded with a shard down", got)
	}
}

// rewindLogScript marks the last n log.sql statements as not yet applied.
func rewindLogScript(t *testing.T, conn clickhouse.Conn, n int) {
	t.Helper()
	scripts, err := getSQLFile(sql.LogScript)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(context.Background(), "SELECT k, max(ver) FROM ver GROUP BY k")
	if err != nil {
		t.Fatal(err)
	}
	vers := map[uint64]uint64{}
	for rows.Next() {
		var k, v uint64
		if err := rows.Scan(&k, &v); err != nil {
			t.Fatal(err)
		}
		vers[k] = v
	}
	rows.Close()
	vers[1] = uint64(len(scripts) - n)
	ctrlExec(t, conn, "TRUNCATE TABLE ver")
	for k, v := range vers {
		ctrlExec(t, conn, fmt.Sprintf("INSERT INTO ver (k, ver) VALUES (%d, %d)", k, v))
	}
}

// TestStartupRecoversTheMVBeforeTheMigration: an older schema whose MV swap
// was interrupted, leaving metrics_15s_mv_bak alone or no MV at all, upgrades:
// one MV feeds metrics_15s afterwards, with start_count, and a cutoff is
// recorded.
func TestStartupRecoversTheMVBeforeTheMigration(t *testing.T) {
	for _, left := range []string{"bak", "none"} {
		t.Run(left, func(t *testing.T) {
			const db = "ctrl_sc_recover"
			conn := ctrlConn(t, db)
			ctrlSchema(t, conn, db)
			insertLines(t, conn, time.Now().UnixNano())
			oldMV(t, conn, "")
			ctrlExec(t, conn, "ALTER TABLE metrics_15s DROP COLUMN start_count")
			// The column and the MODIFY QUERY are log.sql's last two statements.
			rewindLogScript(t, conn, 2)
			if left == "bak" {
				ctrlExec(t, conn, "RENAME TABLE metrics_15s_mv TO metrics_15s_mv_bak")
			} else {
				ctrlExec(t, conn, "DROP TABLE metrics_15s_mv")
			}
			before := time.Now().UnixNano()
			startup(t, db)
			if mv, bak := views(t, conn); !mv || bak {
				t.Fatalf("views: metrics_15s_mv %v, metrics_15s_mv_bak %v", mv, bak)
			}
			if !strings.Contains(mvCreate(t, conn, db), "start_count") {
				t.Fatal("MV without start_count")
			}
			if got := startCountMarker(t, conn, false); got < upgradeCutoff(0, before) {
				t.Fatalf("cutoff %d before %d", got, upgradeCutoff(0, before))
			}
			if count, starts := cellCounts(t, conn); count != 3 || starts != 2 {
				t.Fatalf("count %d, start_count %d, want 3, 2", count, starts)
			}
		})
	}
}

// TestStartupDropsASecondMV: a swap interrupted after the new MV was created
// leaves two views; startup keeps metrics_15s_mv alone.
func TestStartupDropsASecondMV(t *testing.T) {
	const db = "ctrl_sc_second"
	conn := ctrlConn(t, db)
	startup(t, db)
	createMV(t, conn, "metrics_15s_mv_bak", "", metrics15sStartCountCol)
	startup(t, db)
	if mv, bak := views(t, conn); !mv || bak {
		t.Fatalf("views: metrics_15s_mv %v, metrics_15s_mv_bak %v", mv, bak)
	}
	if count, starts := cellCounts(t, conn); count != 3 || starts != 2 {
		t.Fatalf("count %d, start_count %d, want 3, 2", count, starts)
	}
}

// TestStartupFinishesAnInterruptedSwap: a swap interrupted after renaming the
// MV without start_count to metrics_15s_mv_bak is finished at startup, with a
// later cutoff.
func TestStartupFinishesAnInterruptedSwap(t *testing.T) {
	const db = "ctrl_sc_reswap"
	conn := ctrlConn(t, db)
	startup(t, db)
	if got := startCountMarker(t, conn, false); got != 0 {
		t.Fatalf("fresh install: cutoff %d, want 0", got)
	}
	oldMV(t, conn, "")
	ctrlExec(t, conn, "RENAME TABLE metrics_15s_mv TO metrics_15s_mv_bak")
	before := time.Now().UnixNano()
	startup(t, db)
	if mv, bak := views(t, conn); !mv || bak {
		t.Fatalf("views: metrics_15s_mv %v, metrics_15s_mv_bak %v", mv, bak)
	}
	if !strings.Contains(mvCreate(t, conn, db), "start_count") {
		t.Fatal("MV without start_count")
	}
	if got := startCountMarker(t, conn, false); got < upgradeCutoff(0, before) {
		t.Fatalf("cutoff %d before %d", got, upgradeCutoff(0, before))
	}
	if count, starts := cellCounts(t, conn); count != 3 || starts != 2 {
		t.Fatalf("count %d, start_count %d, want 3, 2", count, starts)
	}
}

// TestRecordMetrics15sStartCountCreatesAMissingMV: with metrics_15s and no MV,
// the recorder creates the MV and records a later cutoff.
func TestRecordMetrics15sStartCountCreatesAMissingMV(t *testing.T) {
	const db = "ctrl_sc_nomv"
	conn := ctrlConn(t, db)
	ctrlSchema(t, conn, db)
	if err := RecordMetrics15sStartCount(conn, db, "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	ctrlExec(t, conn, "DROP TABLE metrics_15s_mv")
	before := time.Now().UnixNano()
	if err := RecordMetrics15sStartCount(conn, db, "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	if mv, bak := views(t, conn); !mv || bak {
		t.Fatalf("views: metrics_15s_mv %v, metrics_15s_mv_bak %v", mv, bak)
	}
	if !strings.Contains(mvCreate(t, conn, db), "start_count") {
		t.Fatal("MV without start_count")
	}
	if got := startCountMarker(t, conn, false); got < upgradeCutoff(0, before) {
		t.Fatalf("cutoff %d before %d", got, upgradeCutoff(0, before))
	}
}

// TestStartupAfterAnInterruptedFreshInstall: a fresh install stopped after
// log.sql created metrics_15s, before its MV, starts up with one MV with
// start_count.
func TestStartupAfterAnInterruptedFreshInstall(t *testing.T) {
	const db = "ctrl_sc_interrupted"
	conn := ctrlConn(t, db)
	scripts, err := getSQLFile(sql.LogScript)
	if err != nil {
		t.Fatal(err)
	}
	created := -1
	for i, q := range scripts {
		if strings.Contains(q, "CREATE TABLE IF NOT EXISTS {{.DB}}.metrics_15s ") {
			created = i
			break
		}
	}
	if created < 0 {
		t.Fatal("no metrics_15s CREATE in log.sql")
	}
	exec := getDBExec(conn, map[string]string{"DB": db, "OnCluster": " ", "CREATE_SETTINGS": "",
		"SAMPLES_ORDER_RUL": "timestamp_ns", "MergeTree": "MergeTree", "ReplacingMergeTree": "ReplacingMergeTree",
		"AggregatingMergeTree": "AggregatingMergeTree"}, logger.Logger)
	ctrlExec(t, conn, "CREATE TABLE ver (k UInt64, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY k")
	for _, q := range scripts[:created+1] {
		if err := exec(q); err != nil {
			t.Fatal(err)
		}
	}
	ctrlExec(t, conn, fmt.Sprintf("INSERT INTO ver (k, ver) VALUES (1, %d)", created+1))
	if mv, _ := views(t, conn); mv {
		t.Fatal("MV created before its statement")
	}
	startup(t, db)
	if mv, bak := views(t, conn); !mv || bak {
		t.Fatalf("views: metrics_15s_mv %v, metrics_15s_mv_bak %v", mv, bak)
	}
	if !strings.Contains(mvCreate(t, conn, db), "start_count") {
		t.Fatal("MV without start_count")
	}
	if count, starts := cellCounts(t, conn); count != 3 || starts != 2 {
		t.Fatalf("count %d, start_count %d, want 3, 2", count, starts)
	}
}
