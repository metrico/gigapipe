// Package fakeclickhouse is an in-memory stand-in for the reader's ClickHouse connection in
// tests: each query is answered by a handler and recorded.
package fakeclickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync"

	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/reader/model"
)

// Result is the answer to one query: column names and rows of driver values.
type Result struct {
	Columns []string
	Rows    [][]driver.Value
}

// Handler answers a query.
type Handler func(query string) (Result, error)

// DB records every query and answers it with its handler. It implements model.ISqlxDB and
// model.IDBRegistry.
type DB struct {
	handler Handler
	db      *sql.DB
	mtx     sync.Mutex
	queries []string
}

func New(handler Handler) *DB {
	d := &DB{handler: handler}
	d.db = sql.OpenDB(connector{d})
	return d
}

// Queries returns the queries received so far, in order.
func (d *DB) Queries() []string {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	return append([]string(nil), d.queries...)
}

func (d *DB) answer(query string) (Result, error) {
	d.mtx.Lock()
	d.queries = append(d.queries, query)
	d.mtx.Unlock()
	return d.handler(query)
}

func (d *DB) GetDB(context.Context) (*model.DataDatabasesMap, error) {
	return &model.DataDatabasesMap{Config: &config.ClokiBaseDataBase{}, Session: d}, nil
}
func (d *DB) Run()        {}
func (d *DB) Stop()       {}
func (d *DB) Ping() error { return nil }

func (d *DB) GetName() string { return "fake" }
func (d *DB) QueryCtx(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return d.db.QueryContext(ctx, query, args...)
}
func (d *DB) ExecCtx(context.Context, string, ...any) error {
	return errors.New("fakeclickhouse: exec is not supported")
}
func (d *DB) Conn(ctx context.Context) (*sql.Conn, error) { return d.db.Conn(ctx) }
func (d *DB) Begin() (*sql.Tx, error)                     { return d.db.Begin() }
func (d *DB) Close()                                      {}

type connector struct{ d *DB }

func (c connector) Connect(context.Context) (driver.Conn, error) { return conn(c), nil }
func (c connector) Driver() driver.Driver                        { return nil }

type conn struct{ d *DB }

func (c conn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("fakeclickhouse: prepare is not supported")
}
func (c conn) Close() error              { return nil }
func (c conn) Begin() (driver.Tx, error) { return nil, errors.New("fakeclickhouse: no transactions") }
func (c conn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	res, err := c.d.answer(query)
	if err != nil {
		return nil, err
	}
	return &rows{res: res}, nil
}

type rows struct {
	res Result
	idx int
}

func (r *rows) Columns() []string { return r.res.Columns }
func (r *rows) Close() error      { return nil }
func (r *rows) Next(dest []driver.Value) error {
	if r.idx >= len(r.res.Rows) {
		return io.EOF
	}
	copy(dest, r.res.Rows[r.idx])
	r.idx++
	return nil
}
