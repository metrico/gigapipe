package traceql_transpiler

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jmoiron/sqlx"
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
	"github.com/metrico/qryn/v5/reader/utils/dsn"
	"github.com/metrico/qryn/v5/reader/utils/tables"
	"github.com/metrico/qryn/v5/shared/chtest"
	"github.com/metrico/qryn/v5/shared/distconfig"
)

const (
	// readonlyProbeProfile is the profile of a read-only reader on a cluster.
	readonlyProbeProfile = "readonly = 1, prefer_localhost_replica = 0"
	// probeHTTPAddrEnv names the HTTP address (host:port) of the chtest.AddrEnv server.
	probeHTTPAddrEnv = "GIGAPIPE_TEST_CLICKHOUSE_HTTP"
)

// probeReader reads a searchStore as a user of its own.
type probeReader struct {
	db     model.ISqlxDB
	user   string
	resets *atomic.Int32
}

// probeReader creates a user with settings in its profile. It counts the pool
// resets of the user's connection.
func (s *searchStore) probeReader(t *testing.T, settings string, http bool) *probeReader {
	t.Helper()
	ctx := context.Background()
	user := fmt.Sprintf("chtest_probe_%d", rand.Uint64())
	t.Cleanup(func() {
		_ = s.conn.Exec(ctx, "DROP USER IF EXISTS "+user)
		_ = s.conn.Exec(ctx, "DROP SETTINGS PROFILE IF EXISTS "+user)
	})
	execSQL(t, s.conn, "CREATE SETTINGS PROFILE "+user+" SETTINGS "+settings)
	execSQL(t, s.conn, "CREATE USER "+user+" IDENTIFIED WITH no_password SETTINGS PROFILE '"+user+"'")
	execSQL(t, s.conn, "GRANT SELECT ON "+s.name+".* TO "+user)
	execSQL(t, s.conn, "GRANT REMOTE ON *.* TO "+user)
	opts := &clickhouse.Options{
		Addr: []string{os.Getenv(chtest.AddrEnv)},
		Auth: clickhouse.Auth{Database: s.name, Username: user},
	}
	if http {
		opts.Addr, opts.Protocol = []string{os.Getenv(probeHTTPAddrEnv)}, clickhouse.HTTP
	}
	var opened []*sqlx.DB
	open := func() *sqlx.DB {
		d := sqlx.NewDb(clickhouse.OpenDB(opts), "clickhouse")
		opened = append(opened, d)
		return d
	}
	t.Cleanup(func() {
		for _, d := range opened {
			_ = d.Close()
		}
	})
	resets := &atomic.Int32{}
	db := &dsn.StableSqlxDBWrapper{DB: open(), GetDB: func() *sqlx.DB {
		resets.Add(1)
		return open()
	}}
	return &probeReader{db: db, user: user, resets: resets}
}

// planContext plans the probe of s as r.
func (r *probeReader) planContext(s *searchStore) *shared.PlannerContext {
	ctx := &shared.PlannerContext{
		Ctx: context.Background(), CHDb: r.db, IsCluster: s.cluster != "",
		From: spanStart.Add(-time.Minute), To: spanStart.Add(time.Minute), AncestryRounds: 20,
		VersionInfo: dbversion.VersionInfo{dbversion.CapStructuralQueries: 0},
	}
	distconfig.Init()
	tables.PopulateTableNames(ctx, &model.DataDatabasesMap{Config: &config.ClokiBaseDataBase{
		Name: s.name, ClusterName: s.cluster,
	}})
	return ctx
}

// probeLogs counts the initial queries of user that started in query_log, and
// those among them that carry max_execution_time.
func (s *searchStore) probeLogs(t *testing.T, user string) (started, limited uint64) {
	t.Helper()
	execSQL(t, s.conn, "SYSTEM FLUSH LOGS")
	err := s.conn.QueryRow(context.Background(), `SELECT count(), countIf(Settings['max_execution_time'] != '')
		FROM system.query_log WHERE user = $1 AND is_initial_query AND type = 'QueryStart'`, user).Scan(&started, &limited)
	if err != nil {
		t.Fatal(err)
	}
	return started, limited
}

// testWarnUnresolvedAsReadonlyUser takes a structural search through
// WarnUnresolved, with the process's probe and time limit.
func testWarnUnresolvedAsReadonlyUser(t *testing.T, cluster, http bool) {
	admin := openSearchStore(t, cluster, http)
	admin.seed(t, []span{{41, 1, 1, []string{"a", "b"}}})
	reader := admin.probeReader(t, readonlyProbeProfile, http)
	warns := make(chan string, 1)
	debugs := make(chan string, 1)
	process := processWarner
	t.Cleanup(func() { processWarner = process })
	processWarner = &unresolvedWarner{
		now: process.now, readonly: process.readonly, probe: process.probe, timeout: process.timeout,
		warn:  func(s string) { warns <- s },
		debug: func(a ...any) { debugs <- fmt.Sprint(a...) },
	}
	q, err := traceql_ir.Parse(`{.a = "1"} >> {.b = "1"}`)
	if err != nil {
		t.Fatal(err)
	}
	reqCtx, end := context.WithCancel(context.Background())
	WarnUnresolved(reqCtx, q, reader.planContext(admin))
	end()
	const id41 = "00000000000000000000000000000029"
	select {
	case w := <-warns:
		if !strings.Contains(w, "1 trace(s)") || !strings.Contains(w, "trace_ids=["+id41+"]") {
			t.Errorf("warning %q does not name trace %s", w, id41)
		}
	case d := <-debugs:
		t.Fatalf("probe failed: %s", d)
	case <-time.After(time.Minute):
		t.Fatal("no warning")
	}
	if n := reader.resets.Load(); n != 0 {
		t.Errorf("pool reset %d times", n)
	}
	started, limited := admin.probeLogs(t, reader.user)
	if started == 0 || limited != 0 {
		t.Errorf("probe queries: %d started, %d with max_execution_time", started, limited)
	}
}

func TestWarnUnresolvedAsReadonlyUser(t *testing.T) {
	testWarnUnresolvedAsReadonlyUser(t, false, false)
}

func TestWarnUnresolvedAsReadonlyUserOnCluster(t *testing.T) {
	testWarnUnresolvedAsReadonlyUser(t, true, false)
}

func TestWarnUnresolvedAsReadonlyUserOverHTTP(t *testing.T) {
	testWarnUnresolvedAsReadonlyUser(t, false, true)
}

func TestWarnUnresolvedAsReadonlyUserOverHTTPOnCluster(t *testing.T) {
	testWarnUnresolvedAsReadonlyUser(t, true, true)
}

// slowProbe runs for rows fifths of a second, given max_block_size = 1.
func slowProbe(rows int) string {
	return fmt.Sprintf(`SELECT uniqExact(x + s), groupUniqArray(20)(toString(x + s))
		FROM (SELECT number x, sleepEachRow(0.2) s FROM numbers(%d))`, rows)
}

// probeException waits up to wait for user's failed query in query_log. It
// returns the longest duration and the largest exception code, with ended 0
// when there is none.
func (s *searchStore) probeException(t *testing.T, user string, wait time.Duration) (ended, durationMs uint64, code int32) {
	t.Helper()
	for deadline := time.Now().Add(wait); time.Now().Before(deadline) && ended == 0; time.Sleep(100 * time.Millisecond) {
		execSQL(t, s.conn, "SYSTEM FLUSH LOGS")
		err := s.conn.QueryRow(context.Background(), `SELECT count(), max(query_duration_ms), max(exception_code)
			FROM system.query_log WHERE user = $1 AND is_initial_query AND type = 'ExceptionWhileProcessing'`, user).
			Scan(&ended, &durationMs, &code)
		if err != nil {
			t.Fatal(err)
		}
	}
	return ended, durationMs, code
}

// A readonly=1 user's probe that outruns its time limit ends at the limit, and
// its query ends on the server. Over HTTP the profile has to ask for that.
func testProbeTimeLimitAsReadonlyUser(t *testing.T, http bool) {
	const (
		limit = 1500 * time.Millisecond
		// slowRuns is how long slowProbe(25) runs.
		slowRuns = 5 * time.Second
	)
	admin := openSearchStore(t, false, http)
	profile := readonlyProbeProfile + ", max_block_size = 1"
	if http {
		profile += ", cancel_http_readonly_queries_on_client_close = 1"
	}
	reader := admin.probeReader(t, profile, http)
	var debugged []string
	w := &unresolvedWarner{
		now:      time.Now,
		readonly: readReadonly,
		probe: func(ctx context.Context, _ *traceql_ir.Query, _ *shared.PlannerContext) ([]unresolvedFound, error) {
			return readUnresolved(ctx, reader.db, "j", slowProbe(25), nil)
		},
		warn:    func(s string) { t.Errorf("warned: %s", s) },
		debug:   func(a ...any) { debugged = append(debugged, fmt.Sprint(a...)) },
		timeout: limit,
	}
	start := time.Now()
	w.run(context.Background(), nil, &shared.PlannerContext{CHDb: reader.db})
	if took := time.Since(start); took >= slowRuns-time.Second {
		t.Errorf("probe ended after %v, limit %v", took, limit)
	}
	if len(debugged) != 1 || !strings.Contains(debugged[0], "time limit") {
		t.Errorf("debug lines %q, want one naming the time limit", debugged)
	}
	t.Logf("pool resets on the cancelled probe: %d", reader.resets.Load())
	started, limited := admin.probeLogs(t, reader.user)
	if started == 0 || limited != 0 {
		t.Errorf("probe queries: %d started, %d with max_execution_time", started, limited)
	}
	ended, durationMs, _ := admin.probeException(t, reader.user, slowRuns)
	if ended == 0 || time.Duration(durationMs)*time.Millisecond >= slowRuns-time.Second {
		t.Errorf("server query: %d ended early, longest %d ms", ended, durationMs)
	}
}

func TestProbeTimeLimitAsReadonlyUser(t *testing.T) { testProbeTimeLimitAsReadonlyUser(t, false) }

func TestProbeTimeLimitAsReadonlyUserOverHTTP(t *testing.T) {
	testProbeTimeLimitAsReadonlyUser(t, true)
}

// A readonly=0 user's probe carries max_execution_time from its deadline, and
// the read of readonly does not. Over HTTP the server ends the query with that
// setting, after the reader has stopped waiting.
func testProbeTimeLimitAsWritableUser(t *testing.T, http bool) {
	const (
		limit = 2 * time.Second
		// stopped bounds the server's run of the query: past limit plus the
		// driver's margin, short of slowProbe(100)'s run.
		stopped          = 15 * time.Second
		codeTimeExceeded = 159
	)
	admin := openSearchStore(t, false, http)
	reader := admin.probeReader(t, "readonly = 0, max_block_size = 1", http)
	var debugged []string
	w := &unresolvedWarner{
		now:      time.Now,
		readonly: readReadonly,
		probe: func(ctx context.Context, _ *traceql_ir.Query, _ *shared.PlannerContext) ([]unresolvedFound, error) {
			return readUnresolved(ctx, reader.db, "j", slowProbe(100), nil)
		},
		warn:    func(s string) { t.Errorf("warned: %s", s) },
		debug:   func(a ...any) { debugged = append(debugged, fmt.Sprint(a...)) },
		timeout: limit,
	}
	start := time.Now()
	w.run(context.Background(), nil, &shared.PlannerContext{CHDb: reader.db})
	if took := time.Since(start); took >= stopped/2 {
		t.Errorf("probe ended after %v, limit %v", took, limit)
	}
	if len(debugged) != 1 || !strings.Contains(debugged[0], "time limit") {
		t.Errorf("debug lines %q, want one naming the time limit", debugged)
	}
	ended, durationMs, code := admin.probeException(t, reader.user, stopped)
	if ended == 0 || time.Duration(durationMs)*time.Millisecond >= stopped {
		t.Errorf("server query: %d ended early, longest %d ms", ended, durationMs)
	}
	if http && code != codeTimeExceeded {
		t.Errorf("server query ended with code %d, want %d", code, codeTimeExceeded)
	}
	started, limited := admin.probeLogs(t, reader.user)
	if started < 2 || limited != 1 {
		t.Errorf("queries: %d started, %d with max_execution_time, want the probe's alone", started, limited)
	}
}

func TestProbeTimeLimitAsWritableUser(t *testing.T) { testProbeTimeLimitAsWritableUser(t, false) }

func TestProbeTimeLimitAsWritableUserOverHTTP(t *testing.T) {
	testProbeTimeLimitAsWritableUser(t, true)
}
