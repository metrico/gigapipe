package promql_transpiler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	clconfig "github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/metrico/qryn/v5/reader/utils/tables"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

// selectPlan is the read one selector makes.
type selectPlan struct {
	// Name is the selector's metric name; substitutes have a generated one.
	Name  string
	Func  string
	Route Route
	SQL   string
}

type planOpts struct {
	// tag runs TagGrid with the request's grid before the optimizers.
	tag bool
	// metrics15s makes metrics_15s available, so the optimizers run.
	metrics15s bool
	// ruler tags as the ruler does, leaving step-less subqueries untagged.
	ruler bool
}

var testEngine = promql.NewEngine(promql.EngineOpts{
	MaxSamples:               1 << 30,
	Timeout:                  time.Minute,
	NoStepSubqueryIntervalFn: DefaultSubqueryIntervalMs,
	EnableAtModifier:         true,
})

// planSelects evaluates query on eval the way the query controllers do, with
// a storage that plans each selector's read through TranspileSelect and
// returns no series. It returns the reads in the order the engine makes them.
func planSelects(t *testing.T, query string, eval EvalGrid, opts planOpts) []selectPlan {
	t.Helper()
	var rec *planRecorder
	evalQuery(t, query, eval, opts, func(base shared.PlannerContext, subs map[string]*promql_parser.Substitute,
		vi dbversion.VersionInfo) storage.Querier {
		rec = &planRecorder{t: t, base: base, substitutes: subs, versionInfo: vi}
		return rec
	})
	return rec.plans
}

// evalQuery prepares query as the query controllers do and evaluates it on
// eval over the querier newQuerier returns.
func evalQuery(t *testing.T, query string, eval EvalGrid, opts planOpts,
	newQuerier func(shared.PlannerContext, map[string]*promql_parser.Substitute, dbversion.VersionInfo) storage.Querier,
) parser.Value {
	t.Helper()
	expr, err := promql_parser.Parse(query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	if opts.tag {
		if !opts.ruler {
			eval.SubqueryStepMs = DefaultSubqueryIntervalMs
		}
		TagGrid(expr.Expr, eval)
	}
	vi := dbversion.VersionInfo{}
	if opts.metrics15s {
		vi[dbversion.CapMetrics15s] = 1
		if expr, err = TranspileExpressionV2(expr); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	var base shared.PlannerContext
	tables.PopulateTableNames(&base, &model.DataDatabasesMap{Config: &clconfig.ClokiBaseDataBase{}})
	querier := newQuerier(base, expr.Substitutes, vi)
	queryable := storage.QueryableFunc(func(int64, int64) (storage.Querier, error) { return querier, nil })

	var q promql.Query
	if eval.StepMs == 0 {
		q, err = testEngine.NewInstantQuery(context.Background(), queryable, nil, expr.Expr.String(),
			time.UnixMilli(eval.StartMs))
	} else {
		q, err = testEngine.NewRangeQuery(context.Background(), queryable, nil, expr.Expr.String(),
			time.UnixMilli(eval.StartMs), time.UnixMilli(eval.EndMs), time.Duration(eval.StepMs)*time.Millisecond)
	}
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	res := q.Exec(context.Background())
	if res.Err != nil {
		t.Fatalf("%s: %v", query, res.Err)
	}
	return res.Value
}

type planRecorder struct {
	t           *testing.T
	base        shared.PlannerContext
	substitutes map[string]*promql_parser.Substitute
	versionInfo dbversion.VersionInfo
	plans       []selectPlan
}

func (r *planRecorder) Select(_ context.Context, _ bool, hints *storage.SelectHints,
	matchers ...*labels.Matcher) storage.SeriesSet {
	fn := hints.Func
	resp, err := TranspileSelect(r.base, SelectRequest{
		Hints: hints, Matchers: matchers, Substitutes: r.substitutes, VersionInfo: r.versionInfo,
	})
	if err != nil {
		r.t.Fatal(err)
	}
	str, err := resp.Query.String(&sql.Ctx{Params: map[string]sql.SQLObject{}})
	if err != nil {
		r.t.Fatal(err)
	}
	p := selectPlan{Func: fn, Route: resp.Route, SQL: str}
	for _, m := range matchers {
		if m.Name == labels.MetricName {
			p.Name = m.Value
		}
	}
	r.plans = append(r.plans, p)
	return storage.EmptySeriesSet()
}

func (*planRecorder) LabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (*planRecorder) LabelNames(context.Context, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (*planRecorder) Close() error { return nil }

// routes returns the route of every selector read, by metric name, with all
// substitutes under RouteSubstitute.
func routes(plans []selectPlan) map[string]Route {
	got := map[string]Route{}
	for _, p := range plans {
		if p.Route == RouteSubstitute {
			got[string(RouteSubstitute)] = RouteSubstitute
			continue
		}
		got[p.Name] = p.Route
	}
	return got
}

// selectCase is a query and request with the pinned SHA-256 of the SQL of
// each untagged read the engine makes for it, in order.
type selectCase struct {
	Name       string   `json:"name"`
	Query      string   `json:"query"`
	StartMs    int64    `json:"startMs"`
	EndMs      int64    `json:"endMs"`
	StepMs     int64    `json:"stepMs"`
	Metrics15s bool     `json:"metrics15s"`
	SQLSha256  []string `json:"sqlSha256"`
}

func loadSelectCases(t *testing.T) []selectCase {
	t.Helper()
	b, err := os.ReadFile("testdata/untagged_selects.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []selectCase
	if err := json.Unmarshal(b, &cases); err != nil {
		t.Fatal(err)
	}
	return cases
}

func checkSelectHashes(t *testing.T, c selectCase, plans []selectPlan) {
	t.Helper()
	if len(plans) != len(c.SQLSha256) {
		t.Fatalf("%s: %d reads, want %d", c.Name, len(plans), len(c.SQLSha256))
	}
	for i, p := range plans {
		h := sha256.Sum256([]byte(p.SQL))
		if got := hex.EncodeToString(h[:]); got != c.SQLSha256[i] {
			t.Errorf("%s: read %d (%s) differs from the pinned SQL:\n%s", c.Name, i, p.Route, p.SQL)
		}
	}
}

func (c selectCase) eval() EvalGrid {
	return EvalGrid{StartMs: c.StartMs, EndMs: c.EndMs, StepMs: c.StepMs}
}

func TestTranspileSelectUntaggedSQLIsPinned(t *testing.T) {
	for _, c := range loadSelectCases(t) {
		t.Run(c.Name, func(t *testing.T) {
			checkSelectHashes(t, c, planSelects(t, c.Query, c.eval(), planOpts{metrics15s: c.Metrics15s}))
		})
	}
}

// rawRegridded are the raw reads whose untagged bucket keys miss the
// selector's evaluation points: a bare selector at a step above the lookback,
// an @ off the lattice, and a step-less subquery.
var rawRegridded = map[string]bool{
	"gx range3600 m15s=False":                     true,
	"gx offset 7m range3600 m15s=False":           true,
	"abs(gy) range3600 m15s=False":                true,
	"gx @ 1700000000 range60 m15s=False":          true,
	"gx @ 1700000000 range300 m15s=False":         true,
	"gx @ 1700000000 range3600 m15s=False":        true,
	"gx @ 1700000000 instant m15s=False":          true,
	"max_over_time(gy[1h:]) range60 m15s=False":   true,
	"max_over_time(gy[1h:]) range300 m15s=False":  true,
	"max_over_time(gy[1h:]) range3600 m15s=False": true,
	"max_over_time(gy[1h:]) instant m15s=False":   true,
}

// A tagged selector on the 15s lattice whose function does not need sample
// timestamps reads exactly what it reads untagged, unless it reads raw and
// the untagged keys miss its grid (rawRegridded).
func TestTranspileSelectOnLatticeSQLIsPinned(t *testing.T) {
	routedRaw := []string{"irate(", "deriv(", "idelta(", "@ 1700000000"}
	n, regridded := 0, 0
	for _, c := range loadSelectCases(t) {
		skip := rawRegridded[c.Name]
		if skip {
			regridded++
		}
		for _, s := range routedRaw {
			skip = skip || (c.Metrics15s && strings.Contains(c.Query, s))
		}
		if skip {
			continue
		}
		n++
		t.Run(c.Name, func(t *testing.T) {
			checkSelectHashes(t, c, planSelects(t, c.Query, c.eval(), planOpts{tag: true, metrics15s: c.Metrics15s}))
		})
	}
	if n < 100 || regridded != len(rawRegridded) {
		t.Fatalf("%d cases checked, %d of %d regridded cases found", n, regridded, len(rawRegridded))
	}
}

// selectStartMs is an hour boundary.
const selectStartMs = int64(1_699_999_200_000)

func TestTranspileSelectRoutesOffLatticeRaw(t *testing.T) {
	rng := func(offsetMs, stepMs int64) EvalGrid {
		return EvalGrid{StartMs: selectStartMs + offsetMs, EndMs: selectStartMs + offsetMs + 6*3_600_000, StepMs: stepMs}
	}
	instant := func(offsetMs int64) EvalGrid {
		return EvalGrid{StartMs: selectStartMs + offsetMs, EndMs: selectStartMs + offsetMs}
	}
	const (
		raw = RouteRaw
		ds  = RouteMetrics15s
		sub = RouteSubstitute
	)
	cases := []struct {
		name  string
		query string
		eval  EvalGrid
		want  map[string]Route
	}{
		{"aligned bare selector", "gx", rng(0, 60_000), map[string]Route{"gx": ds}},
		{"phase off the lattice", "gx", rng(7_000, 60_000), map[string]Route{"gx": raw}},
		{"millisecond phase", "gx", rng(15_001, 60_000), map[string]Route{"gx": raw}},
		{"phase on the lattice", "gx", rng(45_000, 60_000), map[string]Route{"gx": ds}},
		{"gcd of step and lookback off the lattice", "gx", rng(0, 20_000), map[string]Route{"gx": raw}},
		{"gcd of step and range off the lattice", "last_over_time(gx[100s])", rng(0, 60_000),
			map[string]Route{"gx": raw}},
		{"offset off the lattice", "gx offset 7s + gy offset 7m", rng(0, 60_000),
			map[string]Route{"gx": raw, "gy": ds}},
		{"@ off the lattice", "gx @ 1700000007 + gy @ 1700000100", rng(0, 3_600_000),
			map[string]Route{"gx": raw, "gy": ds}},
		{"subquery inner offset off the lattice",
			"max_over_time((gx offset 7s)[1h:1m]) + max_over_time((gy offset 1m)[1h:1m])", rng(0, 3_600_000),
			map[string]Route{"gx": raw, "gy": ds}},
		{"instant query off the lattice", "gx", instant(7_000), map[string]Route{"gx": raw}},
		{"instant query on the lattice", "gx", instant(30_000), map[string]Route{"gx": ds}},
		{"aligned irate, deriv and idelta read raw",
			"irate(gc[15m]) + deriv(gy[15m]) + idelta(gx[15m])", rng(0, 3_600_000),
			map[string]Route{"gc": raw, "gy": raw, "gx": raw}},
		{"irate at step 300", "irate(gc[15m])", rng(0, 300_000), map[string]Route{"gc": raw}},
		{"instant irate", "irate(gc[15m])", instant(0), map[string]Route{"gc": raw}},
		{"aligned rate is substituted", "rate(gc[1h])", rng(0, 3_600_000), map[string]Route{"substitute": sub}},
		{"rate off the lattice reads raw", "rate(gc[1h])", rng(7_000, 3_600_000), map[string]Route{"gc": raw}},
		{"aggregate off the lattice reads raw", "sum by (l) (gy)", rng(7_000, 300_000),
			map[string]Route{"gy": raw}},
		{"instant rate off the lattice reads raw", "rate(gc[1h])", instant(7_000), map[string]Route{"gc": raw}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := routes(planSelects(t, c.query, c.eval, planOpts{tag: true, metrics15s: true}))
			if len(got) != len(c.want) {
				t.Fatalf("%s: got %v, want %v", c.query, got, c.want)
			}
			for name, want := range c.want {
				if got[name] != want {
					t.Errorf("%s: %s: got %q, want %q", c.query, name, got[name], want)
				}
			}
		})
	}
}

// Untagged selectors route on their hints alone, irate included.
func TestTranspileSelectUntaggedRouting(t *testing.T) {
	eval := EvalGrid{StartMs: selectStartMs + 7_000, EndMs: selectStartMs + 7_000 + 3_600_000, StepMs: 60_000}
	got := routes(planSelects(t, "irate(gc[15m]) + gx", eval, planOpts{metrics15s: true}))
	if got["gc"] != RouteMetrics15s || got["gx"] != RouteMetrics15s {
		t.Errorf("got %v, want both on metrics_15s", got)
	}
}

// The substitute optimizers decline an off-lattice selector and leave it a
// plain selector under its original function.
func TestOptimizersDeclineOffLatticeSelector(t *testing.T) {
	for _, query := range []string{"rate(gc[1h])", "avg_over_time(gy[1h])", "sum by (l) (gy)", "sum(rate(gc[15m]))"} {
		for _, c := range []struct {
			offsetMs int64
			want     int
		}{{0, 1}, {7_000, 0}} {
			expr, err := promql_parser.Parse(query)
			if err != nil {
				t.Fatal(err)
			}
			TagGrid(expr.Expr, EvalGrid{StartMs: selectStartMs + c.offsetMs, EndMs: selectStartMs + 86_400_000,
				StepMs: 3_600_000})
			want := expr.Expr.String()
			if expr, err = TranspileExpressionV2(expr); err != nil {
				t.Fatal(err)
			}
			if len(expr.Substitutes) != c.want {
				t.Errorf("%s at +%dms: %d substitutes, want %d", query, c.offsetMs, len(expr.Substitutes), c.want)
			}
			if c.want == 0 && expr.Expr.String() != want {
				t.Errorf("%s at +%dms: rewritten to %s", query, c.offsetMs, expr.Expr.String())
			}
		}
	}
}
