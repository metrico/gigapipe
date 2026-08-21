package goldentest

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/reader/model"
)

// RecordingDB is a recording fake DB session satisfying both model.IDBRegistry
// and model.ISqlxDB: the two interfaces a harness needs to substitute for a
// real ClickHouse connection so the reader's real query engine can be driven
// end-to-end (real routing decisions, real post-processing) while capturing
// the SQL it would have sent, instead of any network I/O.
//
// Zero-row results without a real database: QueryCtx/ExecCtx are backed by a
// genuine database/sql.DB opened against an in-process database/sql/driver
// implementation (via sql.OpenDB(driver.Connector), not sql.Register — each
// RecordingDB gets its own isolated *sql.DB, so parallel tests never share
// driver state). *sql.Rows has no public constructor, so this is the only
// way to hand calling code a real, scannable *sql.Rows: by default its
// driver.Rows.Next returns io.EOF immediately, which database/sql surfaces
// as an empty result set — rows.Next() is false, no column data is ever
// scanned, and nothing errors. See SetRows to override that for the rare
// query pattern whose post-processing needs to see typed row data.
//
// Use NewRecordingDB, drive whatever code takes a model.IDBRegistry /
// model.ServiceData.Session, then call Statements() for the captured SQL in
// call order.
type RecordingDB struct {
	mu         sync.Mutex
	statements []string
	canned     []cannedRows

	sqlDB *sql.DB
	dbMap *model.DataDatabasesMap
}

var _ model.IDBRegistry = (*RecordingDB)(nil)
var _ model.ISqlxDB = (*RecordingDB)(nil)

type cannedRows struct {
	match   func(query string) bool
	columns []string
	rows    [][]driver.Value
}

// NewRecordingDB constructs a ready-to-use recording fake. No real network
// connection is ever attempted.
func NewRecordingDB() *RecordingDB {
	r := &RecordingDB{}
	r.sqlDB = sql.OpenDB(&fakeConnector{db: r})
	r.dbMap = &model.DataDatabasesMap{
		Config:  &config.ClokiBaseDataBase{},
		DSN:     "goldentest://recording",
		Session: r,
	}
	return r
}

// Statements returns every query string passed to QueryCtx/ExecCtx, in call
// order. Args, when present, are appended stringified after the query text.
func (r *RecordingDB) Statements() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.statements))
	copy(out, r.statements)
	return out
}

// Reset clears the recorded statement log (canned rows registered via
// SetRows are kept).
func (r *RecordingDB) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statements = nil
}

// SetRows registers canned, typed row data for every future query whose text
// satisfies match. Registrations are consulted in registration order; the
// first match wins. Use this when a caller's post-processing chokes on a
// truly empty result set (e.g. it reads a specific column to decide whether
// to continue) and needs to see shaped data instead.
func (r *RecordingDB) SetRows(match func(query string) bool, columns []string, rows [][]driver.Value) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.canned = append(r.canned, cannedRows{match: match, columns: columns, rows: rows})
}

// SetRowsForSubstring is a convenience wrapper around SetRows matching any
// query containing substr.
func (r *RecordingDB) SetRowsForSubstring(substr string, columns []string, rows [][]driver.Value) {
	r.SetRows(func(q string) bool { return strings.Contains(q, substr) }, columns, rows)
}

func (r *RecordingDB) rowsFor(query string) ([]string, [][]driver.Value) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.canned {
		if c.match(query) {
			return c.columns, c.rows
		}
	}
	return nil, nil
}

func (r *RecordingDB) record(query string, args []any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(args) == 0 {
		r.statements = append(r.statements, query)
		return
	}
	r.statements = append(r.statements, fmt.Sprintf("%s -- args: %v", query, args))
}

// --- model.ISqlxDB ---

func (r *RecordingDB) GetName() string { return "goldentest" }

func (r *RecordingDB) QueryCtx(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	r.record(query, args)
	return r.sqlDB.QueryContext(ctx, query, args...)
}

func (r *RecordingDB) ExecCtx(ctx context.Context, query string, args ...any) error {
	r.record(query, args)
	_, err := r.sqlDB.ExecContext(ctx, query, args...)
	return err
}

func (r *RecordingDB) Conn(ctx context.Context) (*sql.Conn, error) {
	return r.sqlDB.Conn(ctx)
}

func (r *RecordingDB) Begin() (*sql.Tx, error) {
	return r.sqlDB.Begin()
}

func (r *RecordingDB) Close() {
	_ = r.sqlDB.Close()
}

// --- model.IDBRegistry ---

func (r *RecordingDB) GetDB(ctx context.Context) (*model.DataDatabasesMap, error) {
	return r.dbMap, nil
}

func (r *RecordingDB) Run() {}

func (r *RecordingDB) Stop() {}

func (r *RecordingDB) Ping() error { return nil }

// --- database/sql/driver plumbing: an in-process driver whose only job is to
// hand database/sql a real, scannable, empty-by-default *sql.Rows. ---

type fakeConnector struct{ db *RecordingDB }

func (c *fakeConnector) Connect(context.Context) (driver.Conn, error) {
	return &fakeConn{db: c.db}, nil
}

func (c *fakeConnector) Driver() driver.Driver { return &fakeDriver{db: c.db} }

type fakeDriver struct{ db *RecordingDB }

func (d *fakeDriver) Open(string) (driver.Conn, error) { return &fakeConn{db: d.db}, nil }

type fakeConn struct{ db *RecordingDB }

var _ driver.Conn = (*fakeConn)(nil)
var _ driver.QueryerContext = (*fakeConn)(nil)
var _ driver.ExecerContext = (*fakeConn)(nil)

func (c *fakeConn) Prepare(query string) (driver.Stmt, error) {
	return &fakeStmt{db: c.db, query: query}, nil
}

func (c *fakeConn) Close() error { return nil }

func (c *fakeConn) Begin() (driver.Tx, error) { return fakeTx{}, nil }

func (c *fakeConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	cols, rows := c.db.rowsFor(query)
	return &fakeRows{cols: cols, rows: rows}, nil
}

func (c *fakeConn) ExecContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
	return driver.RowsAffected(0), nil
}

type fakeStmt struct {
	db    *RecordingDB
	query string
}

func (s *fakeStmt) Close() error  { return nil }
func (s *fakeStmt) NumInput() int { return -1 }

func (s *fakeStmt) Exec([]driver.Value) (driver.Result, error) {
	return driver.RowsAffected(0), nil
}

func (s *fakeStmt) Query([]driver.Value) (driver.Rows, error) {
	cols, rows := s.db.rowsFor(s.query)
	return &fakeRows{cols: cols, rows: rows}, nil
}

type fakeTx struct{}

func (fakeTx) Commit() error   { return nil }
func (fakeTx) Rollback() error { return nil }

// fakeRows is an empty-by-default driver.Rows: with no rows registered, Next
// returns io.EOF on the very first call, which database/sql surfaces as a
// well-formed, immediately-exhausted result set.
type fakeRows struct {
	cols []string
	rows [][]driver.Value
	pos  int
}

func (r *fakeRows) Columns() []string { return r.cols }
func (r *fakeRows) Close() error      { return nil }

func (r *fakeRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.pos])
	r.pos++
	return nil
}
