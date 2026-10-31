package clickhouse_transpiler

import (
	"context"
	stdsql "database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// fakeConn answers every query with no rows, or with err when the query
// mentions "fail".
type fakeConn struct{ err error }

func (fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (fakeConn) Close() error                        { return nil }
func (fakeConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }
func (c fakeConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(q, "fail") {
		return nil, c.err
	}
	return noRows{}, nil
}

type noRows struct{}

func (noRows) Columns() []string         { return []string{"x"} }
func (noRows) Close() error              { return nil }
func (noRows) Next([]driver.Value) error { return io.EOF }

type fakeConnector struct{ conn fakeConn }

func (c fakeConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (fakeConnector) Driver() driver.Driver                          { return nil }

// fakeDB records the statements it runs.
type fakeDB struct {
	model.ISqlxDB
	db      *stdsql.DB
	mu      sync.Mutex
	queries []string
}

func (f *fakeDB) QueryCtx(ctx context.Context, q string, args ...any) (*stdsql.Rows, error) {
	f.mu.Lock()
	f.queries = append(f.queries, q)
	f.mu.Unlock()
	return f.db.QueryContext(ctx, q, args...)
}

// rawPlanner selects Col.
type rawPlanner struct{ Col string }

func (r rawPlanner) Process(*shared.PlannerContext) (sql.ISelect, error) {
	return sql.NewSelect().Select(sql.NewRawObject(r.Col)), nil
}

func fakeContext(t *testing.T, err error) (*shared.PlannerContext, *fakeDB) {
	t.Helper()
	db := &fakeDB{db: stdsql.OpenDB(fakeConnector{fakeConn{err}})}
	t.Cleanup(func() { _ = db.db.Close() })
	ctx := testPlannerContext(3)
	ctx.Ctx, ctx.CHDb = context.Background(), db
	return ctx, db
}

// The statement after empty ids runs or is skipped as asked, and runs alone
// without ids statements.
func TestQueryOnEmptyIds(t *testing.T) {
	ids := []IdsStatement{{Table: SearchResultTable, Statement: rawPlanner{"ids"}}}
	for _, c := range []struct {
		name    string
		ids     []IdsStatement
		onEmpty OnEmptyIds
		want    []string
	}{
		{"run", ids, RunOnEmptyIds, []string{"ids", "main"}},
		{"skip", ids, SkipOnEmptyIds, []string{"ids"}},
		{"no ids", nil, SkipOnEmptyIds, []string{"main"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, db := fakeContext(t, nil)
			err := Query(ctx, c.ids, nil, rawPlanner{"main"}, c.onEmpty, func(*stdsql.Rows) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			if len(db.queries) != len(c.want) {
				t.Fatalf("ran %q, want %q", db.queries, c.want)
			}
			for i, q := range db.queries {
				if !strings.Contains(q, c.want[i]) {
					t.Errorf("ran %q, want %q", db.queries, c.want)
				}
			}
		})
	}
}

// An unresolved-ancestry failure of either statement is the plain message.
func TestQueryMapsUnresolvedAncestry(t *testing.T) {
	msg := unresolvedAncestryMessage(ancestryRounds(testPlannerContext(3)))
	chErr := errors.New("code: 395, message: " + msg)
	for name, s := range map[string][2]string{"ids": {"fail", "main"}, "main": {"ids", "fail"}} {
		t.Run(name, func(t *testing.T) {
			ctx, _ := fakeContext(t, chErr)
			ids := []IdsStatement{{Table: SearchResultTable, Statement: rawPlanner{s[0]}}}
			err := Query(ctx, ids, nil, rawPlanner{s[1]}, RunOnEmptyIds, func(*stdsql.Rows) error { return nil })
			if err == nil || err.Error() != msg {
				t.Errorf("got %v, want %q", err, msg)
			}
		})
	}
}
