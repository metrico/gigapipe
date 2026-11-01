package maintenance

import (
	"context"
	"sync/atomic"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Statement is one migrator statement. Op and Table identify its step; QueryID and Settings
// travel with the query, and Progress receives the rows read and written so far.
type Statement struct {
	Op       string
	Table    string
	SQL      string
	Args     []any
	Settings map[string]any
	QueryID  string
	Progress func(read, written uint64)
}

// Executor runs the migrator's statements.
type Executor interface {
	// Exec runs a statement and returns the rows it wrote.
	Exec(ctx context.Context, s Statement) (written uint64, err error)
	// Scalar reads one row into dest.
	Scalar(ctx context.Context, s Statement, dest ...any) error
	// Rows calls row for each result row with a function scanning it into dest.
	Rows(ctx context.Context, s Statement, row func(scan func(dest ...any) error) error) error
}

// NewClickhouseExecutor runs migrator statements on conn.
func NewClickhouseExecutor(conn clickhouse.Conn) Executor {
	return &clickhouseExecutor{conn: conn}
}

type clickhouseExecutor struct {
	conn clickhouse.Conn
}

func (e *clickhouseExecutor) context(ctx context.Context, s Statement, written *atomic.Uint64) context.Context {
	var read atomic.Uint64
	opts := []clickhouse.QueryOption{clickhouse.WithProgress(func(p *clickhouse.Progress) {
		r, w := read.Add(p.Rows), written.Add(p.WroteRows)
		if s.Progress != nil {
			s.Progress(r, w)
		}
	})}
	if s.QueryID != "" {
		opts = append(opts, clickhouse.WithQueryID(s.QueryID))
	}
	if len(s.Settings) > 0 {
		opts = append(opts, clickhouse.WithSettings(clickhouse.Settings(s.Settings)))
	}
	return clickhouse.Context(ctx, opts...)
}

func (e *clickhouseExecutor) Exec(ctx context.Context, s Statement) (uint64, error) {
	var written atomic.Uint64
	err := e.conn.Exec(e.context(ctx, s, &written), s.SQL, s.Args...)
	return written.Load(), err
}

func (e *clickhouseExecutor) Scalar(ctx context.Context, s Statement, dest ...any) error {
	var written atomic.Uint64
	return e.conn.QueryRow(e.context(ctx, s, &written), s.SQL, s.Args...).Scan(dest...)
}

func (e *clickhouseExecutor) Rows(ctx context.Context, s Statement, row func(scan func(dest ...any) error) error) error {
	var written atomic.Uint64
	rows, err := e.conn.Query(e.context(ctx, s, &written), s.SQL, s.Args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err = row(rows.Scan); err != nil {
			return err
		}
	}
	return rows.Err()
}
