package promql_transpiler

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/reader/config"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

// rawRead is a raw HintsPlanner read recovered from its SQL.
type rawRead struct {
	// FromNs < timestamp_ns <= ToNs.
	FromNs, ToNs int64
	// Filter keeps ts with (ts-FilterPhase) mod FilterStep == 0, or above
	// FilterMin (at or above it when FilterClosed).
	Filter                             bool
	FilterPhase, FilterStep, FilterMin int64
	FilterClosed                       bool
	// Bucketed reads key every sample to intDiv(ts-Anchor+Width-1, Width)*Width+Anchor
	// and keep the newest one per key.
	Bucketed      bool
	Anchor, Width int64
}

var (
	reRawBounds = regexp.MustCompile(`\(\(samples\.timestamp_ns\) > \((\d+)\)\) and \(\(samples\.timestamp_ns\) <= \((\d+)\)\)`)
	reRawFilter = regexp.MustCompile(`\(\(\(\(?timestamp_ms(?: - (\d+)\))? % (\d+)\) == \(0\)\) or \(\(\(?timestamp_ms(?: - \d+\))? % \d+\) (>=|>) \((\d+)\)\)\)`)
	reRawBucket = regexp.MustCompile(`intDiv\(spls\.timestamp_ms - (\d+) \+ (\d+) - 1, (\d+)\) \* (\d+) \+ (\d+) as timestamp_ms`)
)

func parseRawRead(t *testing.T, s string) rawRead {
	t.Helper()
	var r rawRead
	m := reRawBounds.FindStringSubmatch(s)
	if m == nil {
		t.Fatalf("no read bounds in %s", s)
	}
	r.FromNs, r.ToNs = atoi(t, m[1]), atoi(t, m[2])
	if m := reRawFilter.FindStringSubmatch(s); m != nil {
		r.Filter = true
		if m[1] != "" {
			r.FilterPhase = atoi(t, m[1])
		}
		r.FilterStep, r.FilterMin, r.FilterClosed = atoi(t, m[2]), atoi(t, m[4]), m[3] == ">="
	}
	if m := reRawBucket.FindStringSubmatch(s); m != nil {
		r.Bucketed = true
		r.Anchor, r.Width = atoi(t, m[1]), atoi(t, m[2])
		if m[2] != m[3] || m[3] != m[4] || m[1] != m[5] {
			t.Fatalf("inconsistent bucket key in %s", m[0])
		}
		if r.Filter && strings.Index(s, "% "+strconv.FormatInt(r.FilterStep, 10)) > strings.Index(s, m[0]) {
			t.Fatalf("filter applies after bucketing in %s", s)
		}
	}
	return r
}

func atoi(t *testing.T, s string) int64 {
	t.Helper()
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// apply returns the rows the read returns over samples.
func (r rawRead) apply(samples []model.Sample) []model.Sample {
	var out []model.Sample
	for _, s := range samples {
		ns := s.TimestampMs * 1_000_000
		if ns <= r.FromNs || ns > r.ToNs {
			continue
		}
		if r.Filter {
			mod := (s.TimestampMs - r.FilterPhase) % r.FilterStep
			if (mod != 0 && mod < r.FilterMin) || (mod == r.FilterMin && !r.FilterClosed) {
				continue
			}
		}
		if !r.Bucketed {
			out = append(out, s)
			continue
		}
		key := (s.TimestampMs-r.Anchor+r.Width-1)/r.Width*r.Width + r.Anchor
		if n := len(out); n > 0 && out[n-1].TimestampMs == key {
			out[n-1].Value = s.Value
			continue
		}
		out = append(out, model.Sample{TimestampMs: key, Value: s.Value})
	}
	return out
}

// rawSeries is the data both queriers serve.
type rawSeries struct {
	labels  labels.Labels
	samples []model.Sample
}

// rawFixture is two gauges 5m apart with jitter that puts samples on, just
// before and just after unaligned evaluation points, plus a 2h gap.
func rawFixture() []rawSeries {
	jitter := []int64{0, 7_000, 20_000, 250, 299_999, 61_000, 1_234, 7_001}
	base := selectStartMs - 3*3_600_000
	var out []rawSeries
	for li, l := range []string{"a", "b"} {
		var samples []model.Sample
		for i := int64(0); i < 12*14; i++ {
			if i >= 12*5 && i < 12*7 {
				continue
			}
			ts := base + i*300_000 + jitter[(i+int64(li)*3)%int64(len(jitter))]
			samples = append(samples, model.Sample{TimestampMs: ts, Value: float64((i*i*7+i*3+int64(li)*5)%41) - 6.5})
		}
		out = append(out, rawSeries{labels.FromStrings("__name__", "gx", "l", l), samples})
	}
	return out
}

// fixedLabels serves one label set for every fingerprint.
type fixedLabels struct{ lbls labels.Labels }

func (f fixedLabels) Get(uint64) model.Labels {
	var out model.Labels
	f.lbls.Range(func(l labels.Label) { out = append(out, l) })
	return out
}

func (f fixedLabels) GetNative(uint64) labels.Labels { return f.lbls }

// dataQuerier serves the fixture. As raw, it returns every sample in the
// hints' range, as a Prometheus TSDB would. Otherwise it plans the read
// through TranspileSelect and returns what the planned raw SQL would.
type dataQuerier struct {
	t           *testing.T
	raw         bool
	base        shared.PlannerContext
	substitutes map[string]*promql_parser.Substitute
	versionInfo dbversion.VersionInfo
	data        []rawSeries
	reads       []rawRead
}

func (q *dataQuerier) Select(_ context.Context, _ bool, hints *storage.SelectHints,
	matchers ...*labels.Matcher) storage.SeriesSet {
	read := rawRead{FromNs: (hints.Start - 1) * 1_000_000, ToNs: hints.End * 1_000_000}
	fn := hints.Func
	prolong := false
	if !q.raw {
		resp, err := TranspileSelect(q.base, SelectRequest{
			Hints: hints, Matchers: matchers, Substitutes: q.substitutes, VersionInfo: q.versionInfo,
		})
		if err != nil {
			q.t.Fatal(err)
		}
		if resp.Route != RouteRaw {
			q.t.Fatalf("%s read from %s", fn, resp.Route)
		}
		str, err := resp.Query.String(&sql.Ctx{Params: map[string]sql.SQLObject{}})
		if err != nil {
			q.t.Fatal(err)
		}
		read = parseRawRead(q.t, str)
		q.reads = append(q.reads, read)
		// Bare, rate, deriv and delta reads in a range query carry stale markers.
		prolong = (fn == "" || fn == "deriv" || fn == "rate" || fn == "delta") && hints.Step != 0
	}
	set := &model.SeriesSet{}
	for _, s := range q.data {
		if !matchesAll(s.labels, matchers) {
			continue
		}
		samples := read.apply(s.samples)
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

func matchesAll(l labels.Labels, matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		if !m.Matches(l.Get(m.Name)) {
			return false
		}
	}
	return true
}

func (*dataQuerier) LabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (*dataQuerier) LabelNames(context.Context, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (*dataQuerier) Close() error { return nil }

// rawGrids are range queries at unaligned starts, millisecond starts and steps
// that do not divide the start, plus instant queries and the ruler's
// unaligned instant evaluation.
func rawGrids() []struct {
	name string
	eval EvalGrid
	opts planOpts
} {
	type g = struct {
		name string
		eval EvalGrid
		opts planOpts
	}
	tagged := planOpts{tag: true}
	var out []g
	for _, off := range []int64{0, 7_000, 60_250, 1_234_567} {
		for _, step := range []int64{7_000, 60_000, 300_000, 420_000, 3_600_000} {
			span := min(6*3_600_000, 400*step)
			out = append(out, g{fmt.Sprintf("range +%dms step %dms", off, step),
				EvalGrid{StartMs: selectStartMs + off, EndMs: selectStartMs + off + span + 1_234, StepMs: step}, tagged})
		}
		out = append(out, g{fmt.Sprintf("instant +%dms", off),
			EvalGrid{StartMs: selectStartMs + 3_600_000 + off, EndMs: selectStartMs + 3_600_000 + off}, tagged})
	}
	for _, at := range []int64{7_000, 299_999, 3_600_000 + 1_234_567} {
		out = append(out, g{fmt.Sprintf("ruler +%dms", at),
			EvalGrid{StartMs: selectStartMs + at, EndMs: selectStartMs + at}, planOpts{tag: true, ruler: true}})
	}
	return out
}

var rawQueries = []string{
	"gx",
	"gx offset 7m",
	"gx offset 7s",
	"gx @ 1699999810.250",
	"abs(gx)",
	"sum(gx)",
	"max_over_time(gx[5m])",
	"last_over_time(gx[100s])",
	"absent_over_time(gx[5m])",
	"absent_over_time(gx[30s])",
	"rate(gx[15m])",
	"max_over_time(gx[1h:5m])",
	"max_over_time((gx offset 7s)[1h:1m])",
	"rate(gx[15m:1m])",
}

// A tagged selector read raw returns exactly what the engine computes over
// every raw sample, on any grid.
func TestTranspileSelectRawMatchesRawSamples(t *testing.T) {
	data := rawFixture()
	for _, g := range rawGrids() {
		for _, query := range rawQueries {
			t.Run(g.name+" "+query, func(t *testing.T) {
				want := evalData(t, query, g.eval, g.opts, data, true)
				got := evalData(t, query, g.eval, g.opts, data, false)
				if got != want {
					t.Fatalf("got\n%s\nwant\n%s", got, want)
				}
			})
		}
	}
}

// Under Compat_4_0_19 hints.Start is not floored, and raw reads still match.
func TestTranspileSelectRawMatchesRawSamplesCompat4019(t *testing.T) {
	setting := &config.Cloki.Setting.ClokiReader.Compat_4_0_19
	saved := *setting
	*setting = true
	t.Cleanup(func() { *setting = saved })
	data := rawFixture()
	for _, g := range rawGrids() {
		for _, query := range []string{"gx", "gx offset 7s", "max_over_time(gx[5m])", "max_over_time(gx[1h:5m])"} {
			t.Run(g.name+" "+query, func(t *testing.T) {
				want := evalData(t, query, g.eval, g.opts, data, true)
				if got := evalData(t, query, g.eval, g.opts, data, false); got != want {
					t.Fatalf("got\n%s\nwant\n%s", got, want)
				}
			})
		}
	}
}

func evalData(t *testing.T, query string, eval EvalGrid, opts planOpts, data []rawSeries, raw bool) string {
	t.Helper()
	v := evalQuery(t, query, eval, opts, func(base shared.PlannerContext, subs map[string]*promql_parser.Substitute,
		vi dbversion.VersionInfo) storage.Querier {
		return &dataQuerier{t: t, raw: raw, base: base, substitutes: subs, versionInfo: vi, data: data}
	})
	return v.String()
}

// Every evaluation point of a tagged bare selector read raw is a bucket key,
// and the lookback edge T-5m is a bucket edge unless a trailing filter already
// bounds the bucket to (T-5m, T].
func TestTranspileSelectRawKeysAreEvaluationPoints(t *testing.T) {
	for _, g := range rawGrids() {
		for _, query := range []string{"gx", "gx offset 7s", "abs(gx)", "max_over_time(gx[1h:5m])"} {
			t.Run(g.name+" "+query, func(t *testing.T) {
				points := evalPoints(g.eval, query)
				for _, p := range planSelects(t, query, g.eval, g.opts) {
					r := parseRawRead(t, p.SQL)
					if !r.Bucketed {
						t.Fatalf("%s: not bucketed:\n%s", query, p.SQL)
					}
					if r.Anchor*1_000_000 > r.FromNs {
						t.Errorf("anchor %d above the read bound %d", r.Anchor, r.FromNs/1_000_000)
					}
					for _, T := range points {
						if (T-r.Anchor)%r.Width != 0 {
							t.Fatalf("point %d is not a key (anchor %d, width %d)", T, r.Anchor, r.Width)
						}
					}
					if r.Filter {
						if r.FilterClosed || r.FilterStep != r.Width || r.FilterStep-r.FilterMin != model.LookbackDeltaMs {
							t.Errorf("filter %+v does not keep (T-5m, T] of width %d", r, r.Width)
						}
					} else if model.LookbackDeltaMs%r.Width != 0 {
						t.Errorf("width %d does not divide the lookback", r.Width)
					}
				}
			})
		}
	}
}

// evalPoints returns where query's selector is evaluated on eval.
func evalPoints(eval EvalGrid, query string) []int64 {
	var shift int64
	if strings.Contains(query, "offset 7s") {
		shift = 7_000
	}
	var out []int64
	if strings.Contains(query, "[1h:5m]") {
		for _, T := range evalPoints(eval, "gx") {
			for p := (T - 3_600_000) / 300_000 * 300_000; p <= T; p += 300_000 {
				if p > T-3_600_000 && !slices.Contains(out, p) {
					out = append(out, p)
				}
			}
		}
		return out
	}
	if eval.StepMs == 0 {
		return []int64{eval.StartMs - shift}
	}
	for T := eval.StartMs; T <= eval.EndMs; T += eval.StepMs {
		out = append(out, T-shift)
	}
	return out
}

// The Step > Range filter of a tagged range function read raw is phased on
// its grid.
func TestTranspileSelectRawTrailingFilterIsPhased(t *testing.T) {
	cases := []struct {
		query     string
		eval      EvalGrid
		wantPhase int64
	}{
		{"max_over_time(gx[5m])", EvalGrid{StartMs: selectStartMs + 7_000, EndMs: selectStartMs + 86_400_000,
			StepMs: 3_600_000}, 7_000},
		{"absent_over_time(gx[5m])", EvalGrid{StartMs: selectStartMs + 1_234_567, EndMs: selectStartMs + 86_400_000,
			StepMs: 3_600_000}, 1_234_567},
		{"max_over_time(gx[5m] offset 7s)", EvalGrid{StartMs: selectStartMs + 60_000, EndMs: selectStartMs + 86_400_000,
			StepMs: 420_000}, (selectStartMs + 60_000 - 7_000) % 420_000},
		{"max_over_time(gx[5m])", EvalGrid{StartMs: selectStartMs, EndMs: selectStartMs + 86_400_000,
			StepMs: 3_600_000}, 0},
	}
	for _, c := range cases {
		plans := planSelects(t, c.query, c.eval, planOpts{tag: true})
		if len(plans) != 1 {
			t.Fatalf("%s: %d reads, want 1", c.query, len(plans))
		}
		r := parseRawRead(t, plans[0].SQL)
		if !r.Filter || r.FilterPhase != c.wantPhase || r.FilterStep != c.eval.StepMs {
			t.Errorf("%s at %d/%d: filter %+v, want phase %d", c.query, c.eval.StartMs, c.eval.StepMs, r, c.wantPhase)
		}
	}
}
