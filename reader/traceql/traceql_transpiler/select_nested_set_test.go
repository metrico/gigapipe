package traceql_transpiler

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jmoiron/sqlx"
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/ctrl/logger"
	"github.com/metrico/qryn/v5/ctrl/qryn/maintenance"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
	"github.com/metrico/qryn/v5/reader/utils/dsn"
	"github.com/metrico/qryn/v5/reader/utils/tables"
	"github.com/metrico/qryn/v5/shared/chtest"
)

// spanStart is the start of every seeded span.
var spanStart = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

// openTraces gives a migrated database with trace 1: root span 1, with
// attribute a = 1, and its child span 2.
func openTraces(t *testing.T) (clickhouse.Conn, model.ISqlxDB) {
	t.Helper()
	conn, db := chtest.Open(t)
	if err := maintenance.Update(conn, db, "", maintenance.CLUST_MODE_SINGLE, 7, "", "", false, logger.Logger); err != nil {
		t.Fatal(err)
	}
	execSQL(t, conn, fmt.Sprintf(`INSERT INTO traces_input (trace_id, span_id, parent_id, name, timestamp_ns, duration_ns, tags)
		VALUES ('%032x', '%016x', '', 'root', %d, 10, [('name', 'root'), ('a', '1')]),
		('%032x', '%016x', '%016x', 'child', %d, 5, [('name', 'child')])`,
		1, 1, spanStart.UnixNano(), 1, 2, 1, spanStart.UnixNano()+1))
	sqlDB := sqlx.NewDb(clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{os.Getenv(chtest.AddrEnv)},
		Auth: clickhouse.Auth{Database: db},
	}), "clickhouse")
	t.Cleanup(func() { _ = sqlDB.Close() })
	return conn, &dsn.StableSqlxDBWrapper{DB: sqlDB, GetDB: func() *sqlx.DB { return sqlDB }}
}

// execSQL runs q or fails the test.
func execSQL(t *testing.T, conn clickhouse.Conn, q string) {
	t.Helper()
	if err := conn.Exec(context.Background(), q); err != nil {
		t.Fatalf("%v\n%s", err, q)
	}
}

// searchTraces runs a TraceQL search and returns its traces.
func searchTraces(t *testing.T, db model.ISqlxDB, query string) []model.TraceInfo {
	t.Helper()
	q, err := traceql_ir.Parse(query)
	if err != nil {
		t.Fatal(err)
	}
	planner, err := Plan(q)
	if err != nil {
		t.Fatal(err)
	}
	ctx := &shared.PlannerContext{
		Ctx:   context.Background(),
		CHDb:  db,
		From:  spanStart.Add(-time.Minute),
		To:    spanStart.Add(time.Minute),
		Limit: 20,
		// The test server supports structural queries.
		VersionInfo: dbversion.VersionInfo{dbversion.CapStructuralQueries: 0},
	}
	tables.PopulateTableNames(ctx, &model.DataDatabasesMap{Config: &config.ClokiBaseDataBase{}})
	ch, err := planner.Process(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var res []model.TraceInfo
	for traces := range ch {
		res = append(res, traces...)
	}
	return res
}

// search runs a TraceQL search and returns each listed span's attributes as
// key -> "int:v" or "string:v".
func search(t *testing.T, db model.ISqlxDB, query string) map[string]map[string]string {
	t.Helper()
	res := map[string]map[string]string{}
	for _, trace := range searchTraces(t, db, query) {
		if len(trace.SpanSets) != 1 {
			t.Errorf("trace %s has %d spansets", trace.TraceID, len(trace.SpanSets))
		}
		for _, span := range trace.SpanSet.Spans {
			attrs := map[string]string{}
			for _, a := range span.Attributes {
				switch {
				case a.Value.IntValue != nil:
					attrs[a.Key] = "int:" + *a.Value.IntValue
				case a.Value.StringValue != nil:
					attrs[a.Key] = "string:" + *a.Value.StringValue
				}
			}
			res[span.SpanID] = attrs
		}
	}
	return res
}

const selectNestedSet = `{} | select(nestedSetLeft, nestedSetRight, nestedSetParent)`

// zeros is a span's selected values before numbering.
func zeros() map[string]string {
	return map[string]string{"nestedSetLeft": "int:0", "nestedSetRight": "int:0", "nestedSetParent": "int:0"}
}

// assertSpans compares search() results.
func assertSpans(t *testing.T, got, want map[string]map[string]string) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("got  %v\nwant %v", got, want)
	}
}

// Spans of a trace not yet numbered read 0/0/0.
func TestSelectNestedSetOfUnnumberedTrace(t *testing.T) {
	_, db := openTraces(t)
	assertSpans(t, search(t, db, selectNestedSet), map[string]map[string]string{
		"0000000000000001": zeros(),
		"0000000000000002": zeros(),
	})
}

// select() reads the newest stored values; a span without a row reads 0/0/0.
func TestSelectNestedSetReadsNewestValues(t *testing.T) {
	conn, db := openTraces(t)
	execSQL(t, conn, fmt.Sprintf(`INSERT INTO tempo_traces_nested_set (trace_id, span_id, date, left, right, parent, version)
		VALUES (unhex('%032x'), unhex('%016x'), '2026-10-01', 1, 2, -1, 1), (unhex('%032x'), unhex('%016x'), '2026-10-01', 1, 4, -1, 2)`,
		1, 1, 1, 1))
	assertSpans(t, search(t, db, selectNestedSet), map[string]map[string]string{
		"0000000000000001": {"nestedSetLeft": "int:1", "nestedSetRight": "int:4", "nestedSetParent": "int:-1"},
		"0000000000000002": zeros(),
	})
	assertSpans(t, search(t, db, `{name = "child"} | select(nestedSetRight) | count() > 0 | select(name)`),
		map[string]map[string]string{"0000000000000002": {"nestedSetRight": "int:0"}})
	assertSpans(t, search(t, db, `{} | select(.a, nestedSetLeft)`), map[string]map[string]string{
		"0000000000000001": {"a": "string:1", "nestedSetLeft": "int:1"},
		"0000000000000002": {"nestedSetLeft": "int:0"},
	})
}
