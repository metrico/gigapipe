package promql_transpiler

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

// gridRead is a DownsampleGridPlanner read recovered from its SQL.
type gridRead struct {
	FromNs, ToNs int64
	// Keys are intDiv(t-Phase+Width-1, Width)*Width+Phase.
	Phase, Width int64
	// Cells start at c with (c-EdgePhase) mod Edge != 0; raw samples are read
	// from the other cells. Cells is false when no cell is read.
	Cells           bool
	EdgePhase, Edge int64
	// Filter keeps t with (t-FilterPhase) mod FilterStep == 0 or above FilterMin.
	Filter                             bool
	FilterPhase, FilterStep, FilterMin int64
	Merge                              string
	Shift                              int64
	// presence reads a present/absent function, whose value is 1.
	presence bool
}

var (
	reGridKey  = regexp.MustCompile(`intDiv\(intDiv\(samples\.timestamp_ns, 1000000\)(?: - (\d+))? \+ (\d+), (\d+)\) \* (\d+)(?: \+ (\d+))? as key_ms`)
	reGridCell = regexp.MustCompile(`\(\(\(?intDiv\(samples\.timestamp_ns, 1000000\)(?: - (\d+)\))? % (\d+)\) != \(0\)\)`)
	reGridEdge = regexp.MustCompile(`\(\(\(?intDiv\(samples\.timestamp_ns, 15000000000\) \* 15000(?: - (\d+)\))? % (\d+)\) == \(0\)\)`)
	reGridFilt = regexp.MustCompile(`\(\(\(\(?intDiv\(samples\.timestamp_ns, 1000000\)(?: - (\d+)\))? % (\d+)\) == \(0\)\) or \(\(\(?intDiv\(samples\.timestamp_ns, 1000000\)(?: - \d+\))? % \d+\) > \((\d+)\)\)\)`)
	reGridOut  = regexp.MustCompile(`SELECT fingerprint, (.+?) as val, key_ms( - 1)? as timestamp_ms FROM \(\(`)
)

func parseGridRead(t *testing.T, s string) gridRead {
	t.Helper()
	var r gridRead
	m := reRawBounds.FindStringSubmatch(s)
	if m == nil {
		t.Fatalf("no read bounds in %s", s)
	}
	r.FromNs, r.ToNs = atoi(t, m[1]), atoi(t, m[2])
	keys := reGridKey.FindAllStringSubmatch(s, -1)
	if len(keys) == 0 {
		t.Fatalf("no bucket key in %s", s)
	}
	for _, k := range keys {
		if k[0] != keys[0][0] {
			t.Fatalf("parts key differently: %s vs %s", k[0], keys[0][0])
		}
	}
	k := keys[0]
	r.Width = atoi(t, k[3])
	if k[1] != k[5] || k[3] != k[4] || atoi(t, k[2]) != r.Width-1 {
		t.Fatalf("inconsistent bucket key %s", k[0])
	}
	if k[1] != "" {
		r.Phase = atoi(t, k[1])
	}
	edge := reGridEdge.FindStringSubmatch(s)
	if edge == nil {
		t.Fatalf("no edge cells in %s", s)
	}
	if edge[1] != "" {
		r.EdgePhase = atoi(t, edge[1])
	}
	r.Edge = atoi(t, edge[2])
	if cell := reGridCell.FindStringSubmatch(s); cell != nil {
		r.Cells = true
		if cell[1] != edge[1] || cell[2] != edge[2] || !strings.Contains(s, "FROM metrics_15s as samples") {
			t.Fatalf("cells %s do not complement the edges %s", cell[0], edge[0])
		}
	}
	if filters := reGridFilt.FindAllStringSubmatch(s, -1); filters != nil {
		f := filters[0]
		r.Filter = true
		if f[1] != "" {
			r.FilterPhase = atoi(t, f[1])
		}
		r.FilterStep, r.FilterMin = atoi(t, f[2]), atoi(t, f[3])
		if r.Cells && len(filters) != 2 {
			t.Fatalf("the filter applies to one part only in %s", s)
		}
	}
	out := reGridOut.FindStringSubmatch(s)
	if out == nil {
		t.Fatalf("no outer select in %s", s)
	}
	r.Merge = out[1]
	if out[2] != "" {
		r.Shift = -1
	}
	return r
}

func (r gridRead) kept(ms int64) bool {
	if !r.Filter {
		return true
	}
	mod := (ms - r.FilterPhase) % r.FilterStep
	return mod == 0 || mod > r.FilterMin
}

func (r gridRead) key(ms int64) int64 {
	return (ms-r.Phase+r.Width-1)/r.Width*r.Width + r.Phase
}

// gridAgg is one part's aggregate of a bucket.
type gridAgg struct {
	lastTs, auxTs  int64
	last, min, max float64
	sum            float64
	count          int64
	seen           bool
}

func (a *gridAgg) add(ts int64, v float64, auxTs int64) {
	if !a.seen || ts > a.lastTs {
		a.lastTs, a.last = ts, v
	}
	if !a.seen || v < a.min {
		a.min = v
	}
	if !a.seen || v > a.max {
		a.max = v
	}
	a.auxTs = max(a.auxTs, auxTs)
	a.sum += v
	a.count++
	a.seen = true
}

// apply returns the rows the read returns over samples, as metrics_15s cells
// built from them would answer it.
func (r gridRead) apply(t *testing.T, samples []model.Sample) []model.Sample {
	type part struct{ cells, raw map[int64]*gridAgg }
	p := part{map[int64]*gridAgg{}, map[int64]*gridAgg{}}
	agg := func(m map[int64]*gridAgg, k int64) *gridAgg {
		if m[k] == nil {
			m[k] = &gridAgg{}
		}
		return m[k]
	}
	var keys []int64
	for _, s := range samples {
		c := s.TimestampMs / 15_000 * 15_000
		if (c-r.EdgePhase)%r.Edge == 0 {
			ns := s.TimestampMs * 1_000_000
			if ns > r.FromNs && ns <= r.ToNs && r.kept(s.TimestampMs) {
				agg(p.raw, r.key(s.TimestampMs)).add(s.TimestampMs, s.Value, ns)
				keys = append(keys, r.key(s.TimestampMs))
			}
			continue
		}
		ns := c * 1_000_000
		if r.Cells && ns > r.FromNs && ns <= r.ToNs && r.kept(c) {
			agg(p.cells, r.key(c)).add(s.TimestampMs, s.Value, ns)
			keys = append(keys, r.key(c))
		}
	}
	slices.Sort(keys)
	var out []model.Sample
	for _, k := range slices.Compact(keys) {
		parts := []*gridAgg{}
		for _, a := range []*gridAgg{p.cells[k], p.raw[k]} {
			if a != nil {
				parts = append(parts, a)
			}
		}
		var v float64
		switch r.Merge {
		case "argMax(val, aux)":
			best := parts[0]
			for _, a := range parts[1:] {
				if a.auxTs > best.auxTs {
					best = a
				}
			}
			v = best.last
		case "max(val)":
			v = parts[0].max
			for _, a := range parts[1:] {
				v = max(v, a.max)
			}
			if r.presence {
				v = 1
			}
		case "min(val)":
			v = parts[0].min
			for _, a := range parts[1:] {
				v = min(v, a.min)
			}
		case "sum(val)", "sum(val) / sum(aux)":
			var sum float64
			var n int64
			for _, a := range parts {
				sum += a.sum
				n += a.count
			}
			v = sum
			if r.Merge != "sum(val)" {
				v = sum / float64(n)
			}
		default:
			t.Fatalf("unknown merge %q", r.Merge)
		}
		out = append(out, model.Sample{TimestampMs: k + r.Shift, Value: v})
	}
	return out
}

// gridQuerier serves fixture data through TranspileSelect, returning what the
// planned raw or metrics_15s SQL would.
type gridQuerier struct {
	t           *testing.T
	base        shared.PlannerContext
	substitutes map[string]*promql_parser.Substitute
	versionInfo dbversion.VersionInfo
	data        []rawSeries
	routes      map[Route]int
}

func (q *gridQuerier) Select(_ context.Context, _ bool, hints *storage.SelectHints,
	matchers ...*labels.Matcher) storage.SeriesSet {
	fn := hints.Func
	resp, err := TranspileSelect(q.base, SelectRequest{
		Hints: hints, Matchers: matchers, Substitutes: q.substitutes, VersionInfo: q.versionInfo,
	})
	if err != nil {
		q.t.Fatal(err)
	}
	str, err := resp.Query.String(&sql.Ctx{Params: map[string]sql.SQLObject{}})
	if err != nil {
		q.t.Fatal(err)
	}
	q.routes[resp.Route]++
	var apply func([]model.Sample) []model.Sample
	switch resp.Route {
	case RouteRaw:
		apply = parseRawRead(q.t, str).apply
	case RouteMetrics15s:
		r := parseGridRead(q.t, str)
		r.presence = fn == "absent_over_time" || fn == "present_over_time"
		apply = func(s []model.Sample) []model.Sample { return r.apply(q.t, s) }
	default:
		q.t.Fatalf("%s read from %s", fn, resp.Route)
	}
	prolong := (fn == "" || fn == "deriv" || fn == "rate" || fn == "delta") && hints.Step != 0
	set := &model.SeriesSet{}
	for _, s := range q.data {
		if !matchesAll(s.labels, matchers) {
			continue
		}
		samples := apply(s.samples)
		if len(samples) == 0 {
			continue
		}
		set.Series = append(set.Series, &model.SeriesV2{
			LabelsGetter: fixedLabels{s.labels}, Samples: samples, Prolong: prolong, StepMs: hints.Step,
		})
	}
	set.Reset()
	return set
}

func (*gridQuerier) LabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (*gridQuerier) LabelNames(context.Context, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (*gridQuerier) Close() error { return nil }

// gridFixture is rawFixture plus a dense gauge sampled every 10s with ms
// jitter, several samples to a cell.
func gridFixture() []rawSeries {
	out := rawFixture()
	base := selectStartMs - 3*3_600_000
	var samples []model.Sample
	for i := int64(0); i < 6*14*360; i++ {
		ts := base + i*10_000 + (i*137)%1000
		samples = append(samples, model.Sample{TimestampMs: ts, Value: float64((i*i*13+i*5)%29) - 3.25})
	}
	return append(out, rawSeries{labels.FromStrings("__name__", "gd", "l", "a"), samples})
}

// evalGrid evaluates query on eval over data, either as Prometheus would over
// every sample or through the planned reads with metrics_15s available and no
// substitutes.
func evalGrid(t *testing.T, query string, eval EvalGrid, data []rawSeries, raw bool, routes map[Route]int) string {
	t.Helper()
	opts := planOpts{tag: true}
	v := evalQuery(t, query, eval, opts, func(base shared.PlannerContext, subs map[string]*promql_parser.Substitute,
		_ dbversion.VersionInfo) storage.Querier {
		if raw {
			return &dataQuerier{t: t, raw: true, data: data}
		}
		vi := dbversion.VersionInfo{dbversion.CapMetrics15s: 1}
		return &gridQuerier{t: t, base: base, substitutes: subs, versionInfo: vi, data: data, routes: routes}
	})
	return v.String()
}

// latticeGrids are range and instant queries whose phase is on the 15s lattice.
func latticeGrids() []struct {
	name string
	eval EvalGrid
} {
	type g = struct {
		name string
		eval EvalGrid
	}
	var out []g
	for _, off := range []int64{0, 45_000, 1_800_000} {
		for _, step := range []int64{15_000, 60_000, 300_000, 420_000, 3_600_000, 21_600_000} {
			span := min(36*3_600_000, 400*step)
			out = append(out, g{fmt.Sprintf("range +%dms step %dms", off, step),
				EvalGrid{StartMs: selectStartMs + off, EndMs: selectStartMs + off + span, StepMs: step}})
		}
		for _, at := range []int64{0, 3_600_000} {
			out = append(out, g{fmt.Sprintf("instant +%dms", at+off),
				EvalGrid{StartMs: selectStartMs + at + off, EndMs: selectStartMs + at + off}})
		}
	}
	return out
}

var gridQueries = []string{
	"gx",
	"gd",
	"gx offset 7m",
	"gx @ 1700000100",
	"abs(gd)",
	"sum(gx)",
	"max_over_time(gx[5m])",
	"max_over_time(gd[1h])",
	"min_over_time(gd[5m])",
	"sum_over_time(gd[5m])",
	"last_over_time(gd[1h])",
	"absent_over_time(gx[5m])",
	"present_over_time(gx[15m])",
	"max_over_time(gx[1h:5m])",
	"max_over_time((gd offset 1m)[1h:1m])",
	"rate(gd[15m:1m])",
}

// A tagged selector on the lattice read from metrics_15s returns exactly what
// the engine computes over every raw sample.
func TestTranspileSelectDownsampleMatchesRawSamples(t *testing.T) {
	data := gridFixture()
	routes := map[Route]int{}
	for _, g := range latticeGrids() {
		for _, query := range gridQueries {
			t.Run(g.name+" "+query, func(t *testing.T) {
				want := evalGrid(t, query, g.eval, data, true, nil)
				if got := evalGrid(t, query, g.eval, data, false, routes); got != want {
					t.Fatalf("got\n%s\nwant\n%s", got, want)
				}
			})
		}
	}
	if routes[RouteMetrics15s] < 300 {
		t.Fatalf("only %d metrics_15s reads (%v)", routes[RouteMetrics15s], routes)
	}
}

// Every evaluation point of a tagged selector read from metrics_15s is a
// bucket key, and the edge cells are those starting on a key.
func TestTranspileSelectDownsampleKeysAreEvaluationPoints(t *testing.T) {
	for _, g := range latticeGrids() {
		for _, query := range []string{"gx", "abs(gx)", "max_over_time(gx[1h:5m])", "absent_over_time(gx[5m])",
			"max_over_time(gx[1h])"} {
			t.Run(g.name+" "+query, func(t *testing.T) {
				for _, p := range planSelects(t, query, g.eval, planOpts{tag: true, metrics15s: true}) {
					if p.Route != RouteMetrics15s {
						continue
					}
					r := parseGridRead(t, p.SQL)
					for _, T := range evalPoints(g.eval, query) {
						if (T-r.Phase)%r.Width != 0 || (T-r.EdgePhase)%r.Edge != 0 {
							t.Fatalf("point %d: key %d/%d, edge %d/%d", T, r.Phase, r.Width, r.EdgePhase, r.Edge)
						}
					}
					if r.Width%r.Edge != 0 {
						t.Errorf("edge %d does not divide the width %d", r.Edge, r.Width)
					}
				}
			})
		}
	}
}

// oneSample is a series holding a single sample.
func oneSample(ts int64) []rawSeries {
	return []rawSeries{{labels.FromStrings("__name__", "gx", "l", "a"), []model.Sample{{TimestampMs: ts, Value: 42}}}}
}

// A sample inside the cell [T, T+15s) is not seen at T; one exactly at T is.
func TestTranspileSelectDownsampleExcludesTheCellAfterT(t *testing.T) {
	T := selectStartMs + 3_600_000
	cases := []struct {
		query string
		eval  EvalGrid
	}{
		{"gx", EvalGrid{StartMs: T, EndMs: T}},
		{"gx", EvalGrid{StartMs: T - 3_600_000, EndMs: T, StepMs: 300_000}},
		{"max_over_time(gx[5m])", EvalGrid{StartMs: T, EndMs: T + 3_600_000, StepMs: 3_600_000}},
		{"max_over_time(gx[1h:5m])", EvalGrid{StartMs: T, EndMs: T}},
		{"absent_over_time(gx[5m])", EvalGrid{StartMs: T, EndMs: T + 3_600_000, StepMs: 3_600_000}},
	}
	for _, c := range cases {
		for _, ts := range []int64{T + 7_000, T + 1, T + 14_999, T, T - 300_000, T - 299_999} {
			t.Run(fmt.Sprintf("%s sample at T%+dms", c.query, ts-T), func(t *testing.T) {
				routes := map[Route]int{}
				want := evalGrid(t, c.query, c.eval, oneSample(ts), true, nil)
				if got := evalGrid(t, c.query, c.eval, oneSample(ts), false, routes); got != want {
					t.Fatalf("got\n%s\nwant\n%s", got, want)
				}
				if routes[RouteMetrics15s] == 0 {
					t.Fatalf("no metrics_15s read: %v", routes)
				}
			})
		}
	}
}

// At a step above the lookback a bare selector returns nothing at T for a
// sample older than T-5m.
func TestTranspileSelectDownsampleDropsSamplesOlderThanTheLookback(t *testing.T) {
	T := selectStartMs + 3_600_000
	for _, step := range []int64{600_000, 3_600_000, 21_600_000} {
		eval := EvalGrid{StartMs: T, EndMs: T + 2*step, StepMs: step}
		for _, age := range []int64{300_000, 300_001, 315_000, 360_000, step - 15_000} {
			got := evalGrid(t, "gx", eval, oneSample(T-age), false, map[Route]int{})
			if got != "" {
				t.Errorf("step %d, sample at T-%dms: got %s", step, age, got)
			}
		}
		if got := evalGrid(t, "gx", eval, oneSample(T-299_999), false, map[Route]int{}); got == "" {
			t.Errorf("step %d: the sample at T-299999ms is lost", step)
		}
	}
}

// Tagged instant and @ reads use the selector's single-point grid as their
// step: one bucket of the lookback or range, keyed on the point.
func TestTranspileSelectDownsampleSinglePointGrid(t *testing.T) {
	T := selectStartMs + 3_600_000 + 45_000
	cases := []struct {
		query      string
		eval       EvalGrid
		wantWidth  int64
		wantPhase  int64
		wantFilter bool
	}{
		{"gx", EvalGrid{StartMs: T, EndMs: T}, 300_000, T % 300_000, false},
		{"abs(gx)", EvalGrid{StartMs: T, EndMs: T}, 300_000, T % 300_000, false},
		{"absent_over_time(gx[1h])", EvalGrid{StartMs: T, EndMs: T}, 3_600_000, T % 3_600_000, false},
		{"absent_over_time(gx[5m])", EvalGrid{StartMs: T, EndMs: T}, 300_000, T % 300_000, false},
		{"gx @ 1700000100", EvalGrid{StartMs: selectStartMs, EndMs: selectStartMs + 6*3_600_000, StepMs: 60_000},
			300_000, 1_700_000_100_000 % 300_000, false},
	}
	for _, c := range cases {
		plans := planSelects(t, c.query, c.eval, planOpts{tag: true, metrics15s: true})
		if len(plans) != 1 || plans[0].Route != RouteMetrics15s {
			t.Fatalf("%s: %+v", c.query, plans)
		}
		r := parseGridRead(t, plans[0].SQL)
		if r.Width != c.wantWidth || r.Phase != c.wantPhase%r.Width || r.Filter != c.wantFilter {
			t.Errorf("%s: %+v, want width %d phase %d", c.query, r, c.wantWidth, c.wantPhase)
		}
		got := evalGrid(t, c.query, c.eval, gridFixture(), false, map[Route]int{})
		if want := evalGrid(t, c.query, c.eval, gridFixture(), true, nil); got != want || (got == "") != strings.HasPrefix(c.query, "absent") {
			t.Errorf("%s: got\n%s\nwant\n%s", c.query, got, want)
		}
	}
}

// A tagged selector without a range steps on its grid whatever its function.
func TestTranspileSelectSubqueryInnerKeepsGridStep(t *testing.T) {
	eval := EvalGrid{StartMs: selectStartMs, EndMs: selectStartMs + 6*3_600_000, StepMs: 3_600_000}
	for _, query := range []string{"rate(gx[15m:1m])", "changes(gx[15m:1m])", "rate((gx offset 7s)[15m:1m])"} {
		plans := planSelects(t, query, eval, planOpts{tag: true, metrics15s: true})
		if len(plans) != 1 {
			t.Fatalf("%s: %d reads", query, len(plans))
		}
		var width int64
		switch plans[0].Route {
		case RouteMetrics15s:
			width = parseGridRead(t, plans[0].SQL).Width
		case RouteRaw:
			width = parseRawRead(t, plans[0].SQL).Width
		}
		if width != 60_000 {
			t.Errorf("%s (%s): width %d, want 60000", query, plans[0].Route, width)
		}
	}
}
