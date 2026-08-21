package clickhouse_planner_test

// This file drives the LogQL golden-SQL conformance corpus (M1.b): unlike
// PromQL (M1.a), LogQL's clickhouse_planner reduces a parsed query straight
// to a single sql.ISelect with no DB/engine execution required -- "pure
// planner chain -> single SQL string" per the milestone's own scoping. The
// harness therefore calls the exact production sequence
// logql_parser.Parse -> clickhouse_planner.Plan -> plan.Process(ctx) ->
// sel.String(sql.DefaultCtx()), matching the pattern already used by
// reader/promql/promql_transpiler/vector_range_test.go's transpileRangeCtx
// for PromQL's per-substitute SQL, and by this package's own
// planner_labels_joiner_test.go for rendering an ISelect to text.
//
// clickhouse_planner.Plan(script, true) is called directly rather than going
// through the higher-level logql_transpiler.Plan/Transpile entry points,
// because logql_transpiler.Plan only calls clickhouse_planner.Plan for
// queries its own decidePlanMode routes to pure-SQL evaluation; queries
// containing a bare `json`/`logfmt` parser stage or a `line_format` stage
// are instead split and partly executed in Go over already-fetched rows
// (see reader/logql/logql_transpiler/planner.go's GetBreakpoint/breakScript).
// clickhouse_planner.Plan itself has no awareness of that routing -- it is
// the same planner-chain code either way, just invoked directly here so
// every representative query (including ones production would route
// internally) yields one deterministic SQL string with no DB required. Two
// consequences worth calling out for anyone reading the fixtures:
//
//   - `| logfmt` unconditionally fails to plan here (clickhouse_planner's
//     ParserPlanner only implements "regexp" and "json"): see the
//     parser_logfmt_unsupported fixture, pinned via CheckError rather than
//     CheckSQL. This is a genuine, real, deterministic error from the real
//     planner chain, not a hand-typed one.
//   - `| label_format` is accepted by the parser but clickhouse_planner's
//     own planSpl() pipeline-stage switch (planner.go) has no case for
//     *logql_parser.StrSelectorPipeline.LabelFormat at all, so it is
//     silently skipped: the emitted SQL for a query with `| label_format`
//     is identical to the same query without it. The label_format fixtures
//     below intentionally pin this real (if perhaps surprising) current
//     behavior -- if clickhouse_planner ever grows real label_format
//     support, that is exactly the kind of one-line planner change this
//     corpus exists to catch as a fixture diff.
//
// Binary expressions (e.g. `rate(a) / rate(b)`) are a different shape: when
// both operands are individually plannable this way, logql_transpiler's own
// planScriptToSQL/planAtomToSQL (planner.go) do exactly what renderBinary
// below does -- plan each side with clickhouse_planner.Plan and combine them
// with clickhouse_planner.BinaryExprSQLPlanner, which folds both sides into
// one UNION-ALL-based sql.ISelect entirely in SQL (no in-process/Go-side
// merge for this mode; that only happens for planModeInternal, out of scope
// here). So a binary fixture here pins the one real combined SQL statement,
// built from the same exported BinaryExprSQLPlanner type production uses,
// not a hand-assembled approximation. Each operand's own SQL shape is
// already separately pinned by its corresponding non-binary fixture (e.g.
// range_rate_5m for `rate({app="a"}[5m])`).

import (
	"fmt"
	"os"
	"testing"
	"time"

	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/qryn/v5/reader/config"
	"github.com/metrico/qryn/v5/reader/internal/goldentest"
	"github.com/metrico/qryn/v5/reader/logql/logql_parser"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/clickhouse_planner"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

const goldenDir = "testdata/golden"

// TestMain initializes the package-global config.Cloki once before any test
// in this package runs: StreamSelectPlanner (planner_stream_select.go) reads
// config.Cloki.Setting.ClokiReader.OmitEmptyValues directly while building
// SQL, and a nil config.Cloki panics. Mirrors
// reader/promql/promql_transpiler/setup_test.go's TestMain for the same
// reason.
func TestMain(m *testing.M) {
	if config.Cloki == nil {
		config.Cloki = clconfig.New(clconfig.CLOKI_READER, nil, "", "")
	}
	os.Exit(m.Run())
}

// goldenEnd is the fixed reference instant every fixture's time window is
// computed from, so generated SQL (epoch literals, date bounds, ...) is a
// pure function of the fixture's own query text, never of wall-clock time.
var goldenEnd = time.Date(2024, 1, 15, 12, 0, 7, 0, time.UTC)

// goldenCtx builds the single fixed *shared.PlannerContext every fixture in
// this corpus renders against. Unlike PromQL's (query, step, range) keying,
// M1.b fixtures need no per-fixture time-window meta: every duration that
// matters to the emitted SQL (the [5m]-style range, any offset) is already
// part of the query text itself, and clickhouse_planner reads the wall-clock
// window (ctx.From/ctx.To) and Step only to bake fixed literals/bucket
// widths into the SQL, not to change its shape.
//
// Step is deliberately set below every duration used in this corpus's
// queries so StepFixPlanner's intDiv-bucketing branch (planner_step_fix.go)
// never fires here -- it only matters for step values coarser than a
// query's own range, a case this corpus does not need to cover for M1.b.
func goldenCtx() *shared.PlannerContext {
	return &shared.PlannerContext{
		From:                       goldenEnd.Add(-time.Hour),
		To:                         goldenEnd,
		Step:                       time.Second,
		OrderASC:                   false,
		Limit:                      1000,
		Type:                       shared.SAMPLES_TYPE_BOTH,
		SamplesTableName:           "samples_v3",
		SamplesDistTableName:       "samples_v3",
		TimeSeriesTableName:        "time_series",
		TimeSeriesDistTableName:    "time_series",
		TimeSeriesGinTableName:     "time_series_gin",
		TimeSeriesGinDistTableName: "time_series_gin",
		Metrics15sTableName:        "metrics_15s",
		Metrics15sDistTableName:    "metrics_15s",
	}
}

// renderQuery parses query and renders it to one or more golden SQL
// statements via the real clickhouse_planner chain. A binary expression
// (script.IsBinary()) is rendered by renderBinary; anything else by
// renderAtom.
func renderQuery(query string) ([]string, error) {
	script, err := logql_parser.Parse(query)
	if err != nil {
		return nil, err
	}
	if script.IsBinary() {
		return renderBinary(script)
	}
	return renderAtom(script)
}

// renderAtom renders a single non-binary LogQLScript through
// clickhouse_planner.Plan(script, true) -- finalize=true, matching every
// call site logql_transpiler.Plan itself uses for a pure-clickhouse-mode
// script (planner.go).
func renderAtom(script *logql_parser.LogQLScript) ([]string, error) {
	plan, err := clickhouse_planner.Plan(script, true)
	if err != nil {
		return nil, err
	}
	sel, err := plan.Process(goldenCtx())
	if err != nil {
		return nil, err
	}
	str, err := sel.String(sql.DefaultCtx())
	if err != nil {
		return nil, err
	}
	return []string{str}, nil
}

// renderBinary renders a two-operand binary expression (`left <op> right`)
// by planning each side with clickhouse_planner.Plan and combining them with
// clickhouse_planner.BinaryExprSQLPlanner -- the exact same construction
// logql_transpiler's planAtomToSQL/planScriptToSQL use for a pure-SQL binary
// plan (planner.go). Chains of more than one operator are out of scope for
// this corpus (none of the fixtures use them).
func renderBinary(script *logql_parser.LogQLScript) ([]string, error) {
	if len(script.BinOps) != 1 {
		return nil, fmt.Errorf("golden harness: only single binary operator chains are supported here, got %d operators", len(script.BinOps))
	}

	left, err := clickhouse_planner.Plan(&logql_parser.LogQLScript{Head: script.Head}, true)
	if err != nil {
		return nil, err
	}

	op := script.BinOps[0]
	combined := &clickhouse_planner.BinaryExprSQLPlanner{Left: left, Op: op.Op}
	if op.Right.Scalar != "" {
		combined.RightScalar = op.Right.Scalar
	} else {
		right, err := clickhouse_planner.Plan(&logql_parser.LogQLScript{Head: op.Right}, true)
		if err != nil {
			return nil, err
		}
		combined.Right = right
	}

	sel, err := combined.Process(goldenCtx())
	if err != nil {
		return nil, err
	}
	str, err := sel.String(sql.DefaultCtx())
	if err != nil {
		return nil, err
	}
	return []string{str}, nil
}

// TestGolden is the M1.b LogQL golden-SQL corpus. See
// reader/internal/goldentest's package doc comment for the naming
// convention this satisfies.
func TestGolden(t *testing.T) {
	fixtures, err := goldentest.LoadDir(goldenDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(fixtures) == 0 {
		t.Fatalf("no fixtures found under %s", goldenDir)
	}
	for _, f := range fixtures {
		f := f
		t.Run(f.Name, func(t *testing.T) {
			got, err := renderQuery(f.Query)
			if f.WantErr != "" {
				f.CheckError(t, err)
				return
			}
			if err != nil {
				t.Fatalf("%s: unexpected error: %v", f.Name, err)
			}
			f.CheckSQL(t, got...)
		})
	}
}
