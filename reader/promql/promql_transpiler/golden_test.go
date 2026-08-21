package promql_transpiler_test

// This file drives the PromQL golden-SQL conformance corpus (M1.a): it feeds
// every fixture under testdata/golden through the REAL PromQL engine
// (promql.NewEngine + service.CLokiQueriable, backed by goldentest's
// recording fake DB session) so the raw-vs-downsampled routing decision in
// reader/service/prom_queryable.go's transpileLabelMatchers is genuinely
// exercised, never bypassed by calling the transpiler directly with
// hand-built hints.
//
// Fixture keys are (query, step, range): "step" is the range query's
// resolution step, "range" is the query's total time window (end - start).
// Both live in the fixture's "meta" section as durations parseable by
// time.ParseDuration (e.g. "30s", "5m"). This is distinct from any [5m]-style
// duration embedded in the query text itself, which is part of "query".
//
// This file lives in an external test package (promql_transpiler_test, not
// promql_transpiler) specifically so it can import reader/service, which
// itself imports promql_transpiler: an external test package is a separate
// compilation unit from the package under test, so this does not create an
// import cycle. TestMain in setup_test.go (package promql_transpiler)
// initializes the shared config.Cloki global before any test in this
// directory runs, including this one.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/internal/goldentest"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
	"github.com/metrico/qryn/v5/reader/promql/promql_transpiler"
	"github.com/metrico/qryn/v5/reader/service"
	"github.com/prometheus/prometheus/promql"
)

const goldenDir = "testdata/golden"

// goldenEnd is the fixed reference instant every range query's "end" is
// computed from, so generated SQL (WITH FILL boundaries, epoch literals,
// bucket alignment, ...) is a pure function of the fixture's own inputs and
// never of wall-clock time. Deliberately not aligned to a 15s boundary: the
// engine/transpiler are responsible for their own alignment, and a fixture
// pinning that behavior should not rely on the harness pre-aligning it.
var goldenEnd = time.Date(2024, 1, 15, 12, 0, 7, 0, time.UTC)

// newGoldenEngine builds a PromQL engine the same way
// reader/router.NewPromEngine does (see reader/router/prometheus_query_range.go),
// duplicated here rather than imported so this package's test binary does not
// pull in the whole HTTP router dependency graph for a single function call.
// Logger is nil (defaults to a no-op logger inside promql.NewEngine) since
// logging has no effect on the generated SQL.
func newGoldenEngine() *promql.Engine {
	return promql.NewEngine(promql.EngineOpts{
		MaxSamples:    50_000_000,
		Timeout:       30 * time.Second,
		LookbackDelta: 0,
		NoStepSubqueryIntervalFn: func(int64) int64 {
			return time.Minute.Milliseconds()
		},
		EnableAtModifier:     true,
		EnableNegativeOffset: false,
	})
}

// isVersionProbeStatement reports whether stmt is one of the bootstrap
// queries dbversion.GetVersionInfo issues to probe server capabilities.
//
// goldentest.RecordingDB.GetName() always returns the fixed constant
// "goldentest", and GetVersionInfo caches its result process-wide keyed by
// that name -- so in practice this probe only ever reaches a fake once per
// process, no matter how many fixtures (and fresh RecordingDB instances) run.
// But that cache is time-limited (dbversion throttles and clears it 10s after
// first use), so in principle a slow enough run could re-trigger the probe
// for a later fixture. Filtering these statements out, rather than relying on
// every fixture completing within that window, keeps captured SQL a pure
// function of (query, step, range) regardless of how long the run takes.
func isVersionProbeStatement(stmt string) bool {
	return strings.Contains(stmt, "FROM settings") ||
		stmt == "SHOW TABLES" ||
		stmt == "SELECT version()"
}

func filterCaptured(stmts []string) []string {
	out := make([]string, 0, len(stmts))
	for _, s := range stmts {
		if isVersionProbeStatement(s) {
			continue
		}
		out = append(out, s)
	}
	return out
}

// renderPromQL parses and transpiles query exactly as the real
// controller.QueryRange handler does (reader/controller/prom_query_range.go),
// then drives it through the real PromQL engine end-to-end via
// service.CLokiQueriable backed by a fresh recording fake DB, and returns
// every SQL statement the fake captured, in the order the engine issued them.
func renderPromQL(query string, step, rng time.Duration) ([]string, error) {
	expr, err := promql_parser.Parse(query)
	if err != nil {
		return nil, err
	}
	expr, err = promql_transpiler.TranspileExpressionV2(expr)
	if err != nil {
		return nil, err
	}

	db := goldentest.NewRecordingDB()
	svc := service.CLokiQueriable{ServiceData: model.ServiceData{Session: db}}
	ctx := context.Background()
	eng := newGoldenEngine()

	end := goldenEnd
	start := end.Add(-rng)

	q, err := eng.NewRangeQuery(ctx, svc.SetOidAndDB(ctx, expr), nil, expr.Expr.String(), start, end, step)
	if err != nil {
		return nil, err
	}
	defer q.Close()

	res := q.Exec(ctx)
	if res.Err != nil {
		return nil, res.Err
	}
	return filterCaptured(db.Statements()), nil
}

func mustParseDuration(t *testing.T, name, s string) time.Duration {
	t.Helper()
	if s == "" {
		t.Fatalf("missing required meta %q", name)
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		t.Fatalf("meta %q: bad duration %q: %v", name, s, err)
	}
	return d
}

// TestGolden is the M1.a PromQL golden-SQL corpus. See reader/internal/goldentest's
// package doc comment for the fixture format and naming convention this
// satisfies. This head's own fixture-specific meta keys are "step" and
// "range" (see mustParseDuration below); both are required on every fixture.
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
			step := mustParseDuration(t, "step", f.Meta["step"])
			rng := mustParseDuration(t, "range", f.Meta["range"])

			got, err := renderPromQL(f.Query, step, rng)
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
