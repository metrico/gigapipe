package goldentest_test

import (
	"context"
	"database/sql/driver"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/metrico/qryn/v5/reader/internal/goldentest"
)

// recorder is a fake goldentest.TestingT that records failures instead of
// actually failing the enclosing test. A genuine *testing.T propagates a
// subtest's failure up to its parent, which is exactly wrong for the tests
// below that deliberately feed CheckSQL/CheckError a mismatch to prove they
// detect it: using a recorder keeps that "expected failure" from turning
// this self-test suite itself red.
type recorder struct {
	failed bool
	msgs   []string
}

func (r *recorder) Helper() {}
func (r *recorder) Errorf(format string, args ...any) {
	r.failed = true
	r.msgs = append(r.msgs, fmt.Sprintf(format, args...))
}
func (r *recorder) Fatalf(format string, args ...any) {
	r.failed = true
	r.msgs = append(r.msgs, fmt.Sprintf(format, args...))
}

func writeRaw(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestFixtureRoundTrip proves the loader reads back exactly what a fixture
// file on disk says, for both the "sql-N" and "error" flavors, and that
// LoadDir sorts fixtures by file name.
func TestFixtureRoundTrip(t *testing.T) {
	dir := t.TempDir()
	writeRaw(t, dir, "b_second.txtar", "-- meta --\nstep: 15s\nrange: 5m\n-- query --\nsum(rate(x[5m]))\n-- sql-0 --\nSELECT 1\n-- sql-1 --\nSELECT 2\n")
	writeRaw(t, dir, "a_first.txtar", "-- query --\nunsupported_fn(x)\n-- error --\nunsupported function\n")

	fixtures, err := goldentest.LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(fixtures) != 2 {
		t.Fatalf("expected 2 fixtures, got %d", len(fixtures))
	}

	// Sorted by file name: a_first before b_second.
	if fixtures[0].Name != "a_first" || fixtures[1].Name != "b_second" {
		t.Fatalf("expected [a_first b_second], got [%s %s]", fixtures[0].Name, fixtures[1].Name)
	}

	errFixture := fixtures[0]
	if errFixture.Query != "unsupported_fn(x)" {
		t.Errorf("Query = %q", errFixture.Query)
	}
	if errFixture.WantErr != "unsupported function" {
		t.Errorf("WantErr = %q", errFixture.WantErr)
	}
	if len(errFixture.SQL) != 0 {
		t.Errorf("SQL = %v, want empty", errFixture.SQL)
	}

	sqlFixture := fixtures[1]
	if sqlFixture.Query != "sum(rate(x[5m]))" {
		t.Errorf("Query = %q", sqlFixture.Query)
	}
	if sqlFixture.Meta["step"] != "15s" || sqlFixture.Meta["range"] != "5m" {
		t.Errorf("Meta = %v", sqlFixture.Meta)
	}
	if len(sqlFixture.SQL) != 2 || sqlFixture.SQL[0] != "SELECT 1" || sqlFixture.SQL[1] != "SELECT 2" {
		t.Errorf("SQL = %v", sqlFixture.SQL)
	}
}

// TestCheckSQLPassesOnMatch proves CheckSQL reports no failure when the
// produced SQL matches the golden section exactly.
func TestCheckSQLPassesOnMatch(t *testing.T) {
	dir := t.TempDir()
	writeRaw(t, dir, "sample.txtar", "-- query --\nSELECT 1\n-- sql-0 --\nSELECT 1\n")
	fixtures, err := goldentest.LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	ok := t.Run("subtest", func(t *testing.T) {
		fixtures[0].CheckSQL(t, "SELECT 1")
	})
	if !ok {
		t.Fatal("expected CheckSQL to pass on an exact match")
	}
}

// TestFixtureUpdateFlow proves both halves of -update mode: without it, a
// mismatch against a stale golden fails the test with a diff naming the
// fixture; with it, the same mismatch instead rewrites the fixture on disk,
// and a fresh LoadDir sees the regenerated content.
func TestFixtureUpdateFlow(t *testing.T) {
	dir := t.TempDir()
	writeRaw(t, dir, "sample.txtar", "-- meta --\nk: v\n-- query --\nSELECT 1\n-- sql-0 --\nold sql\n")

	fixtures, err := goldentest.LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	f := fixtures[0]

	// Without -update: a mismatch must fail (via the recorder, not a real
	// subtest failure — see the recorder doc comment).
	rec := &recorder{}
	f.CheckSQL(rec, "new sql")
	if !rec.failed {
		t.Fatal("expected CheckSQL to report a failure on a golden mismatch without -update")
	}

	// With -update: the same mismatch must not fail, and must regenerate.
	*goldentest.Update = true
	t.Cleanup(func() { *goldentest.Update = false })
	ok := t.Run("update-regenerates", func(t *testing.T) {
		f.CheckSQL(t, "new sql")
	})
	if !ok {
		t.Fatal("expected CheckSQL to succeed in -update mode")
	}

	reloaded, err := goldentest.LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(reloaded) != 1 || len(reloaded[0].SQL) != 1 || reloaded[0].SQL[0] != "new sql" {
		t.Fatalf("golden was not regenerated on disk: %+v", reloaded)
	}
	// meta and query must survive an -update rewrite untouched.
	if reloaded[0].Meta["k"] != "v" {
		t.Errorf("meta lost across -update rewrite: %v", reloaded[0].Meta)
	}
	if reloaded[0].Query != "SELECT 1" {
		t.Errorf("query lost across -update rewrite: %q", reloaded[0].Query)
	}

	// Regenerating again with no further change must be byte-identical
	// (deterministic emitters => zero diff on a no-op re-run).
	before, err := os.ReadFile(filepath.Join(dir, "sample.txtar"))
	if err != nil {
		t.Fatal(err)
	}
	t.Run("update-again-noop", func(t *testing.T) {
		reloaded[0].CheckSQL(t, "new sql")
	})
	after, err := os.ReadFile(filepath.Join(dir, "sample.txtar"))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatalf("regenerating with no change produced a diff:\nbefore:\n%s\nafter:\n%s", before, after)
	}
}

// TestCheckErrorFlow proves CheckError's match, mismatch and -update paths.
func TestCheckErrorFlow(t *testing.T) {
	dir := t.TempDir()
	writeRaw(t, dir, "sample.txtar", "-- query --\nbad(x)\n-- error --\nunsupported function\n")
	fixtures, err := goldentest.LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	f := fixtures[0]

	ok := t.Run("matches", func(t *testing.T) {
		f.CheckError(t, errString("plan error: unsupported function bad"))
	})
	if !ok {
		t.Fatal("expected CheckError to pass when the error contains the golden substring")
	}

	rec := &recorder{}
	f.CheckError(rec, errString("a completely different failure"))
	if !rec.failed {
		t.Fatal("expected CheckError to report a failure when the error does not contain the golden substring")
	}

	*goldentest.Update = true
	t.Cleanup(func() { *goldentest.Update = false })
	ok = t.Run("update-regenerates", func(t *testing.T) {
		f.CheckError(t, errString("a completely different failure"))
	})
	if !ok {
		t.Fatal("expected CheckError to succeed in -update mode")
	}
	reloaded, err := goldentest.LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded[0].WantErr != "a completely different failure" {
		t.Fatalf("error golden was not regenerated: %q", reloaded[0].WantErr)
	}
}

type errString string

func (e errString) Error() string { return string(e) }

// TestRecordingDBRecordsAndReturnsEmptyRows proves the recording fake logs
// QueryCtx/ExecCtx calls in order and, with no canned rows registered,
// returns a real *sql.Rows that scanning code sees as an empty result set
// rather than an error.
func TestRecordingDBRecordsAndReturnsEmptyRows(t *testing.T) {
	db := goldentest.NewRecordingDB()

	rows, err := db.QueryCtx(context.Background(), "SELECT * FROM samples WHERE fingerprint = 1")
	if err != nil {
		t.Fatalf("QueryCtx: %v", err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("expected zero rows from an unregistered query pattern")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err(): %v", err)
	}

	if err := db.ExecCtx(context.Background(), "ALTER TABLE samples DELETE WHERE 1", "arg1"); err != nil {
		t.Fatalf("ExecCtx: %v", err)
	}

	got := db.Statements()
	want := []string{
		"SELECT * FROM samples WHERE fingerprint = 1",
		"ALTER TABLE samples DELETE WHERE 1 -- args: [arg1]",
	}
	if len(got) != len(want) {
		t.Fatalf("Statements() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Statements()[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	// Both interfaces the reader's DB session seam requires.
	dbMap, err := db.GetDB(context.Background())
	if err != nil {
		t.Fatalf("GetDB: %v", err)
	}
	if dbMap.Session != db {
		t.Fatal("GetDB's DataDatabasesMap.Session must be the RecordingDB itself")
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

// TestRecordingDBReset proves Reset clears the recorded statement log while
// leaving canned rows (registered via SetRows) untouched.
func TestRecordingDBReset(t *testing.T) {
	db := goldentest.NewRecordingDB()
	db.SetRowsForSubstring("FROM time_series", []string{"fingerprint"},
		[][]driver.Value{{int64(42)}})

	if err := db.ExecCtx(context.Background(), "SELECT 1"); err != nil {
		t.Fatalf("ExecCtx: %v", err)
	}
	if got := db.Statements(); len(got) != 1 {
		t.Fatalf("Statements() before Reset = %v, want 1 entry", got)
	}

	db.Reset()

	if got := db.Statements(); len(got) != 0 {
		t.Fatalf("Statements() after Reset = %v, want none", got)
	}

	// Canned rows must survive Reset.
	rows, err := db.QueryCtx(context.Background(), "SELECT * FROM time_series")
	if err != nil {
		t.Fatalf("QueryCtx: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected the canned row registered before Reset to still be served")
	}

	// A statement issued after Reset is recorded from a clean log.
	if err := db.ExecCtx(context.Background(), "SELECT 2"); err != nil {
		t.Fatalf("ExecCtx: %v", err)
	}
	if got := db.Statements(); len(got) != 2 {
		// One for the QueryCtx above, one for this ExecCtx.
		t.Fatalf("Statements() after Reset + new activity = %v, want 2 entries", got)
	}
}

// TestRecordingDBCannedRows proves a test can pre-register typed row data for
// a specific query pattern and have QueryCtx's *sql.Rows actually scan it.
func TestRecordingDBCannedRows(t *testing.T) {
	db := goldentest.NewRecordingDB()
	db.SetRowsForSubstring("FROM time_series", []string{"fingerprint", "name"},
		[][]driver.Value{
			{int64(42), "up"},
			{int64(43), "down"},
		})

	rows, err := db.QueryCtx(context.Background(), "SELECT fingerprint, name FROM time_series")
	if err != nil {
		t.Fatalf("QueryCtx: %v", err)
	}
	defer rows.Close()

	type row struct {
		fp   int64
		name string
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.fp, &r.name); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err(): %v", err)
	}
	want := []row{{42, "up"}, {43, "down"}}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestRunParserPins proves the generic parser-pin helper on a trivial parser
// (a stand-in for the real per-head parsers): a valid query yields a non-nil
// AST and no error, an invalid one yields a non-nil error, and Check runs for
// deeper shape assertions on the valid case.
func TestRunParserPins(t *testing.T) {
	type ast struct{ query string }
	parse := func(q string) (*ast, error) {
		if q == "" {
			return nil, errString("empty query")
		}
		return &ast{query: q}, nil
	}

	checked := false
	goldentest.RunParserPins(t, parse, []goldentest.ParserPinCase[*ast]{
		{
			Name:  "valid",
			Query: "some_query",
			Check: func(t *testing.T, a *ast) {
				checked = true
				if a.query != "some_query" {
					t.Errorf("query = %q", a.query)
				}
			},
		},
		{
			Name:    "invalid",
			Query:   "",
			WantErr: true,
		},
	})
	if !checked {
		t.Fatal("expected Check to run for the valid case")
	}
}
