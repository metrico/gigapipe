package traceql_transpiler

import (
	"context"
	"database/sql/driver"
	"strconv"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/internal/goldentest"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_parser"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
)

// TestGolden is this head's golden-corpus driving test (see
// reader/internal/goldentest's package doc for the naming convention that
// wires it into `just update-golden traceql` and the merge gate).
//
// Every fixture is driven through the *real* two-level TraceQL pipeline:
//
//  1. traceql_transpiler.Plan / PlanTagsV2 / PlanValuesV2 build a
//     TraceQLComplexityEvaluator wired with the real clickhouse_transpiler
//     SQL tree AND the real complexity/routing SQL tree
//     (clickhouse_transpiler.PlanEval).
//  2. Calling .Process(ctx) against a goldentest.RecordingDB drives the
//     genuine runtime routing decision (TraceQLComplexityEvaluator.Process,
//     reader/traceql/traceql_transpiler/complexity_evaluator.go): it issues
//     the complexity/routing SQL first, scans the (fake, scripted) row(s)
//     coming back, and only then picks the real SimpleRequestProcessor or
//     ComplexRequestProcessor path — exactly as production does, minus the
//     live ClickHouse round trip.
//
// Because the routing decision is a genuine live-data question (see the
// package-level comment on COMPLEXITY_THRESHOLD), a fixture that wants the
// "complex" path scripts a canned row for the complexity/routing query via
// RecordingDB.SetRowsForSubstring("_count", ...) — every complexity/routing
// query, for every kind (search/tags/values) and every AST shape, ends in
// `... AS _count` (see clickhouse_transpiler.EvalFinalizerPlanner), so
// matching that substring is a safe, kind-independent way to script it.
// Fixtures that want "simple" need no scripting at all: RecordingDB returns
// a genuinely empty (but well-typed) *sql.Rows by default, which the real
// routing code reads as complexity 0 — always below threshold.
//
// The very first statement RecordingDB records is always that complexity
// query (the routing decision always runs before either request processor
// does anything); fixtures pin only what comes after it — the actual
// trace-search / tag-lookup / value-lookup SQL — since that is "the emitted
// SQL" the M1.c corpus is about, with the routing decision itself pinned
// explicitly via the fixture's "route" meta key instead.
func TestGolden(t *testing.T) {
	fixtures, err := goldentest.LoadDir("testdata/golden")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fixtures {
		f := f
		t.Run(f.Name, func(t *testing.T) {
			renderFixture(t, f)
		})
	}
}

// fixedFrom/fixedTo are a fixed reference window so every fixture's SQL is
// reproducible independent of wall-clock time.
var (
	fixedFrom = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	fixedTo   = time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)
)

// parseIntMeta reads an optional int64-valued meta key from f.Meta, falling
// back to dflt when the key is absent, and failing the test on a value that
// doesn't parse as base-10 int64.
func parseIntMeta(t *testing.T, f *goldentest.Fixture, key string, dflt int64) int64 {
	t.Helper()
	v, ok := f.Meta[key]
	if !ok {
		return dflt
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		t.Fatalf("%s: bad meta %q %q: %v", f.Name, key, v, err)
	}
	return n
}

func renderFixture(t *testing.T, f *goldentest.Fixture) {
	t.Helper()

	kind := f.Meta["kind"]
	if kind == "" {
		kind = "search"
	}

	limit := parseIntMeta(t, f, "limit", 20)

	script, err := traceql_parser.Parse(f.Query)
	if err != nil {
		f.CheckError(t, err)
		return
	}

	db := goldentest.NewRecordingDB()
	// -1: nothing scripted, real routing code reads complexity 0.
	complexity := parseIntMeta(t, f, "complexity", -1)
	if _, ok := f.Meta["complexity"]; ok {
		db.SetRowsForSubstring("_count", []string{"_count"}, [][]driver.Value{{complexity}})
	}

	ctx := &shared.PlannerContext{
		IsCluster:            false,
		From:                 fixedFrom,
		To:                   fixedTo,
		Limit:                limit,
		TracesAttrsTable:     "traces_attrs_gin",
		TracesAttrsDistTable: "traces_attrs_gin_dist",
		TracesTable:          "traces",
		TracesDistTable:      "traces_dist",
		TracesKVTable:        "traces_kv",
		TracesKVDistTable:    "traces_kv_dist",
		VersionInfo:          dbversion.VersionInfo{},
		Ctx:                  context.Background(),
		CHDb:                 db,
	}

	if procErr := drive(kind, script, f.Meta["key"], ctx); procErr != nil {
		f.CheckError(t, procErr)
		return
	}

	stmts := db.Statements()
	if len(stmts) < 2 {
		t.Fatalf("%s: expected at least 2 recorded statements (complexity-eval + search), got %d: %v",
			f.Name, len(stmts), stmts)
	}
	got := stmts[1:] // stmts[0] is always the complexity/routing query; not part of the pinned SQL.

	wantRoute := f.Meta["route"]
	if wantRoute == "" {
		t.Fatalf("%s: fixture is missing required meta \"route\" (simple|complex)", f.Name)
	}
	gotRoute := "simple"
	if complexity >= COMPLEXITY_THRESHOLD {
		gotRoute = "complex"
	}
	if gotRoute != wantRoute {
		t.Errorf("%s: route mismatch: fixture pins %q, computed %q from complexity=%d (threshold=%d)",
			f.Name, wantRoute, gotRoute, complexity, COMPLEXITY_THRESHOLD)
	}

	f.CheckSQL(t, got...)
}

// drive runs one of the three real TraceQL entry points to completion
// (draining whatever channel it returns), returning the first error
// encountered at any stage (plan construction or Process).
func drive(kind string, script *traceql_parser.TraceQLScript, key string, ctx *shared.PlannerContext) error {
	switch kind {
	case "search":
		proc, err := Plan(script)
		if err != nil {
			return err
		}
		ch, err := proc.Process(ctx)
		if err != nil {
			return err
		}
		for range ch {
		}
	case "tags":
		proc, err := PlanTagsV2(script)
		if err != nil {
			return err
		}
		ch, err := proc.Process(ctx)
		if err != nil {
			return err
		}
		for range ch {
		}
	case "values":
		proc, err := PlanValuesV2(script, key)
		if err != nil {
			return err
		}
		ch, err := proc.Process(ctx)
		if err != nil {
			return err
		}
		for range ch {
		}
	default:
		return &shared.NotSupportedError{Msg: "golden_test: unknown fixture kind " + kind}
	}
	return nil
}
