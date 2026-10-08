package logql_transpiler

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"os"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/qryn/v5/reader/config"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
	sqlsel "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

func TestMain(m *testing.M) {
	if config.Cloki == nil {
		config.Cloki = clconfig.New(clconfig.CLOKI_READER, nil, "", "")
	}
	os.Exit(m.Run())
}

// request is one call to the LogQL query API: a range query over [start, end]
// at step, or an instant query at end.
type request struct {
	start, end   time.Time
	step         time.Duration
	instant      bool
	noMetrics15s bool
	// cluster reads through the *_dist tables, as a sharded deployment does.
	cluster bool
}

func rangeReq(start, end time.Time, step time.Duration) request {
	return request{start: start, end: end, step: step}
}

func instantReq(at time.Time) request {
	return request{start: at, end: at, step: time.Second, instant: true}
}

// logLine is a stored log row served by the fake ClickHouse.
type logLine struct {
	fp     uint64
	labels map[string]string
	ts     int64
	line   string
}

// planned is what one request did: the processor at the root of the chain,
// every SQL statement sent, in order, and the result entries.
type planned struct {
	root shared.RequestProcessor
	sql  []string
	out  []shared.LogEntry
}

// recordingDB is a ClickHouse connection that keeps every statement sent.
type recordingDB interface {
	model.ISqlxDB
	statements() []string
}

// runRequest serves query for req as the query API does, against a fake
// ClickHouse that records the SQL and answers with the lines in its read bounds.
func runRequest(t *testing.T, query string, req request, lines []logLine) planned {
	t.Helper()
	fake := newFakeCH(lines)
	defer fake.Close()
	return serveRequest(t, query, req, fake)
}

// serveRequest serves query for req as the query API does, against db.
func serveRequest(t *testing.T, query string, req request, db recordingDB) planned {
	t.Helper()
	chain, err := Transpile(query)
	if err != nil {
		t.Fatalf("Transpile(%q): %v", query, err)
	}
	fromNs, toNs := req.start.UnixNano(), req.end.UnixNano()
	if req.instant {
		fromNs = toNs - 300000000000
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pctx := &shared.PlannerContext{
		From:                       time.Unix(fromNs/1000000000, 0),
		To:                         time.Unix(toNs/1000000000, 0),
		Limit:                      100,
		Ctx:                        ctx,
		CancelCtx:                  cancel,
		CHDb:                       db,
		CHFinalize:                 true,
		Step:                       req.step,
		Instant:                    req.instant,
		CHSqlCtx:                   &sqlsel.Ctx{Params: map[string]sqlsel.SQLObject{}, Result: map[string]sqlsel.SQLObject{}},
		SamplesTableName:           "samples_v3",
		SamplesDistTableName:       "samples_v3",
		TimeSeriesTableName:        "time_series",
		TimeSeriesDistTableName:    "time_series",
		TimeSeriesGinTableName:     "time_series_gin",
		TimeSeriesGinDistTableName: "time_series_gin",
		Metrics15sTableName:        "metrics_15s",
		Metrics15sDistTableName:    "metrics_15s",
	}
	if req.cluster {
		pctx.IsCluster = true
		pctx.SamplesDistTableName = "samples_v3_dist"
		pctx.TimeSeriesDistTableName = "time_series_dist"
		pctx.TimeSeriesGinDistTableName = "time_series_gin_dist"
		pctx.Metrics15sDistTableName = "metrics_15s_dist"
	}
	if req.noMetrics15s {
		pctx.VersionInfo = dbversion.VersionInfo{}
	}
	ch, err := chain[0].Process(pctx, nil)
	if err != nil {
		t.Fatalf("Process(%q): %v", query, err)
	}
	res := planned{root: chain[0]}
	for entries := range ch {
		for _, e := range entries {
			if e.Err == io.EOF {
				continue
			}
			if e.Err != nil {
				t.Fatalf("%q: %v", query, e.Err)
			}
			res.out = append(res.out, e)
		}
	}
	res.sql = db.statements()
	return res
}

// fakeCH is a model.ISqlxDB backed by a database/sql driver that serves lines.
type fakeCH struct {
	db    *sql.DB
	mu    sync.Mutex
	sql   []string
	lines []logLine
}

func newFakeCH(lines []logLine) *fakeCH {
	f := &fakeCH{lines: lines}
	f.db = sql.OpenDB(fakeConnector{f})
	return f
}

func (f *fakeCH) GetName() string { return "fake" }

func (f *fakeCH) QueryCtx(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	f.mu.Lock()
	f.sql = append(f.sql, query)
	f.mu.Unlock()
	return f.db.QueryContext(ctx, query, args...)
}

func (f *fakeCH) ExecCtx(ctx context.Context, query string, args ...any) error {
	_, err := f.db.ExecContext(ctx, query, args...)
	return err
}

func (f *fakeCH) Conn(ctx context.Context) (*sql.Conn, error) { return f.db.Conn(ctx) }
func (f *fakeCH) Begin() (*sql.Tx, error)                     { return f.db.Begin() }
func (f *fakeCH) Close()                                      { f.db.Close() }

func (f *fakeCH) statements() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.sql)
}

var (
	reFromNs = regexp.MustCompile(`\(samples\.timestamp_ns\) >= \((-?\d+)\)`)
	reToNs   = regexp.MustCompile(`\(samples\.timestamp_ns\) < \((-?\d+)\)`)
)

// readBounds returns the [from, to) bounds of a raw log read.
func readBounds(query string) (int64, int64, bool) {
	a, b := reFromNs.FindStringSubmatch(query), reToNs.FindStringSubmatch(query)
	if a == nil || b == nil {
		return 0, 0, false
	}
	from, _ := strconv.ParseInt(a[1], 10, 64)
	to, _ := strconv.ParseInt(b[1], 10, 64)
	return from, to, true
}

func (f *fakeCH) serve(query string) *fakeRows {
	from, to, ok := readBounds(query)
	rows := &fakeRows{}
	if !ok {
		return rows
	}
	for _, l := range f.lines {
		if l.ts >= from && l.ts < to {
			rows.lines = append(rows.lines, l)
		}
	}
	slices.SortStableFunc(rows.lines, func(a, b logLine) int { return int(b.ts - a.ts) })
	return rows
}

type fakeConnector struct{ f *fakeCH }

func (c fakeConnector) Connect(context.Context) (driver.Conn, error) { return fakeConn(c), nil }
func (c fakeConnector) Driver() driver.Driver                        { return fakeDriver{} }

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) { return nil, driver.ErrSkip }

type fakeConn struct{ f *fakeCH }

func (c fakeConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c fakeConn) Close() error                        { return nil }
func (c fakeConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }

func (c fakeConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	return c.f.serve(query), nil
}

type fakeRows struct {
	lines []logLine
	i     int
}

func (r *fakeRows) Columns() []string {
	return []string{"fingerprint", "labels", "string", "timestamp_ns"}
}
func (r *fakeRows) Close() error { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.i >= len(r.lines) {
		return io.EOF
	}
	l := r.lines[r.i]
	r.i++
	dest[0], dest[1], dest[2], dest[3] = l.fp, l.labels, l.line, l.ts
	return nil
}
