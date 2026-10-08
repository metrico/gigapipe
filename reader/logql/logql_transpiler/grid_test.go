package logql_transpiler

import (
	"fmt"
	"io"
	"math"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
)

var gridD0 = time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)

// gridLines holds three jittered streams of uneven density; l=a/pod=1 also
// has a line on every 5m boundary.
func gridLines() []logLine {
	streams := []map[string]string{
		{"job": "g", "l": "a", "pod": "1"},
		{"job": "g", "l": "a", "pod": "2"},
		{"job": "g", "l": "b", "pod": "1"},
	}
	var lines []logLine
	for m := 0; m < 8*60; m++ {
		for si, s := range streams {
			n := ((m*m*7+m*3+si*5)%11)%5 + (m/60+si)%3
			for j := 0; j < n; j++ {
				ts := gridD0.Add(time.Duration(m*60+20+9*j+2*si)*time.Second +
					time.Duration((j*137+si*11)%1000)*time.Millisecond)
				size := (m*31+j*7+si*13)%97 + 1
				lines = append(lines, logLine{fp: uint64(si + 1), labels: s, ts: ts.UnixNano(),
					line: fmt.Sprintf("size=%d pad=%s", size, strings.Repeat("x", (m*13+j*5+si*17)%41))})
			}
		}
		if m%5 == 0 {
			lines = append(lines, logLine{fp: 1, labels: streams[0], ts: gridD0.Add(time.Duration(m) * time.Minute).UnixNano(),
				line: fmt.Sprintf("size=%d pad=%s", m%50+3, strings.Repeat("y", m%23))})
		}
	}
	return lines
}

// refQuery describes a Go-path query for the reference evaluator.
type refQuery struct {
	fn     string
	r      time.Duration
	offset time.Duration
	by     []string // nil keeps every stream label
	agg    string   // vector aggregation by (by) around the range aggregation
	q      float64
	gt     *float64
}

// refGrid returns the evaluation times of req: epoch k*step from the floored
// start to the ceiled end, or the instant time.
func refGrid(req request) []int64 {
	if req.instant {
		return []int64{req.end.UnixNano()}
	}
	step := req.step.Nanoseconds()
	var ts []int64
	for t := floorDiv(req.start.UnixNano(), step) * step; t <= -floorDiv(-req.end.UnixNano(), step)*step; t += step {
		ts = append(ts, t)
	}
	return ts
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

func sizeOf(line string) float64 {
	var v float64
	fmt.Sscanf(line, "size=%g", &v)
	return v
}

func project(labels map[string]string, by []string) map[string]string {
	if by == nil {
		return labels
	}
	out := map[string]string{}
	for _, l := range by {
		if v, ok := labels[l]; ok {
			out[l] = v
		}
	}
	return out
}

func labelsKey(labels map[string]string) string {
	var ks []string
	for k, v := range labels {
		ks = append(ks, k+"="+v)
	}
	sort.Strings(ks)
	return "{" + strings.Join(ks, ",") + "}"
}

func quantileOf(q float64, vs []float64) float64 {
	s := slices.Clone(vs)
	sort.Float64s(s)
	rank := q * float64(len(s)-1)
	lo := math.Max(0, math.Floor(rank))
	hi := math.Min(float64(len(s)-1), lo+1)
	w := rank - math.Floor(rank)
	return s[int(lo)]*(1-w) + s[int(hi)]*w
}

// reference evaluates q at every T over (T-offset-R, T-offset], drops zeros,
// and returns series -> T -> value.
func reference(lines []logLine, q refQuery, times []int64) map[string]map[int64]float64 {
	res := map[string]map[int64]float64{}
	for _, T := range times {
		hi := T - q.offset.Nanoseconds()
		lo := hi - q.r.Nanoseconds()
		type acc struct {
			labels map[string]string
			items  []logLine
		}
		groups := map[string]*acc{}
		for _, l := range lines {
			if l.ts <= lo || l.ts > hi {
				continue
			}
			lb := project(l.labels, q.by)
			if q.agg != "" {
				lb = l.labels
			}
			k := labelsKey(lb)
			if groups[k] == nil {
				groups[k] = &acc{labels: lb}
			}
			groups[k].items = append(groups[k].items, l)
		}
		vals := map[string]float64{}
		labelsOf := map[string]map[string]string{}
		for k, g := range groups {
			slices.SortStableFunc(g.items, func(a, b logLine) int { return int(a.ts - b.ts) })
			var vs []float64
			var n, bytes, sum float64
			for _, it := range g.items {
				n++
				bytes += float64(len(it.line))
				vs = append(vs, sizeOf(it.line))
				sum += sizeOf(it.line)
			}
			rs := q.r.Seconds()
			var v float64
			switch q.fn {
			case "count_over_time":
				v = n
			case "rate":
				v = n / rs
			case "bytes_over_time":
				v = bytes
			case "bytes_rate":
				v = bytes / rs
			case "sum_over_time":
				v = sum
			case "rate_unwrap":
				v = sum / rs
			case "avg_over_time":
				v = sum / n
			case "min_over_time":
				v = slices.Min(vs)
			case "max_over_time":
				v = slices.Max(vs)
			case "first_over_time":
				v = vs[0]
			case "last_over_time":
				v = vs[len(vs)-1]
			case "quantile_over_time":
				v = quantileOf(q.q, vs)
			default:
				panic(q.fn)
			}
			vals[k] = v
			labelsOf[k] = g.labels
		}
		if q.agg != "" {
			groups := map[string][]float64{}
			for k, v := range vals {
				if q.gt != nil && !(v > *q.gt) {
					continue
				}
				sk := labelsKey(project(labelsOf[k], q.by))
				groups[sk] = append(groups[sk], v)
			}
			vals = map[string]float64{}
			for k, vs := range groups {
				var sum float64
				for _, v := range vs {
					sum += v
				}
				switch q.agg {
				case "sum":
					vals[k] = sum
				case "avg":
					vals[k] = sum / float64(len(vs))
				case "count":
					vals[k] = float64(len(vs))
				case "min":
					vals[k] = slices.Min(vs)
				case "max":
					vals[k] = slices.Max(vs)
				}
			}
		} else if q.gt != nil {
			for k, v := range vals {
				if !(v > *q.gt) {
					delete(vals, k)
				}
			}
		}
		for k, v := range vals {
			if v == 0 {
				continue
			}
			if res[k] == nil {
				res[k] = map[int64]float64{}
			}
			res[k][T] = v
		}
	}
	return res
}

func gotSeries(p planned) map[string]map[int64]float64 {
	res := map[string]map[int64]float64{}
	for _, e := range p.out {
		k := labelsKey(e.Labels)
		if res[k] == nil {
			res[k] = map[int64]float64{}
		}
		if _, dup := res[k][e.TimestampNS]; dup {
			res[k][math.MinInt64] = math.NaN()
		}
		res[k][e.TimestampNS] = e.Value
	}
	return res
}

func diffSeries(got, want map[string]map[int64]float64) []string {
	var out []string
	keys := map[string]bool{}
	for k := range got {
		keys[k] = true
	}
	for k := range want {
		keys[k] = true
	}
	var sorted []string
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	for _, k := range sorted {
		ts := map[int64]bool{}
		for t := range got[k] {
			ts[t] = true
		}
		for t := range want[k] {
			ts[t] = true
		}
		var tl []int64
		for t := range ts {
			tl = append(tl, t)
		}
		slices.Sort(tl)
		for _, t := range tl {
			g, gok := got[k][t]
			w, wok := want[k][t]
			at := time.Unix(0, t).UTC().Format("15:04:05.000")
			switch {
			case !wok:
				out = append(out, fmt.Sprintf("%s %s extra %g", k, at, g))
			case !gok:
				out = append(out, fmt.Sprintf("%s %s missing (want %g)", k, at, w))
			case math.Abs(g-w) > 1e-9+1e-9*math.Abs(w):
				out = append(out, fmt.Sprintf("%s %s got %g want %g", k, at, g, w))
			}
		}
	}
	return out
}

func gt(v float64) *float64 { return &v }

const unwrapStages = `| logfmt | unwrap size`

// goPathCases are served by the Go planner: line_format and logfmt are
// breakpoints.
var goPathCases = []struct {
	query string
	ref   refQuery
}{
	{`count_over_time({job="g"} != "@@never@@" | line_format "{{__line__}}" [5m])`, refQuery{fn: "count_over_time", r: 5 * time.Minute}},
	{`rate({job="g"} != "@@never@@" | line_format "{{__line__}}" [15m])`, refQuery{fn: "rate", r: 15 * time.Minute}},
	{`bytes_over_time({job="g"} != "@@never@@" | line_format "{{__line__}}" [5m])`, refQuery{fn: "bytes_over_time", r: 5 * time.Minute}},
	{`bytes_rate({job="g"} != "@@never@@" | line_format "{{__line__}}" [1h])`, refQuery{fn: "bytes_rate", r: time.Hour}},
	{`sum by (l) (count_over_time({job="g"} != "@@never@@" | line_format "{{__line__}}" [5m]))`, refQuery{fn: "count_over_time", r: 5 * time.Minute, by: []string{"l"}, agg: "sum"}},
	{`sum by (l) (rate({job="g"} != "@@never@@" | line_format "{{__line__}}" [1h]))`, refQuery{fn: "rate", r: time.Hour, by: []string{"l"}, agg: "sum"}},
	{`count_over_time({job="g"} != "@@never@@" | line_format "{{__line__}}" [5m] offset 7m)`, refQuery{fn: "count_over_time", r: 5 * time.Minute, offset: 7 * time.Minute}},
	{`sum by (l) (count_over_time({job="g"} != "@@never@@" | line_format "{{__line__}}" [15m] offset 1h))`, refQuery{fn: "count_over_time", r: 15 * time.Minute, offset: time.Hour, by: []string{"l"}, agg: "sum"}},
	{`max by (l) (count_over_time({job="g"} != "@@never@@" | line_format "{{__line__}}" [5m]))`, refQuery{fn: "count_over_time", r: 5 * time.Minute, by: []string{"l"}, agg: "max"}},
	{`min by (l) (rate({job="g"} != "@@never@@" | line_format "{{__line__}}" [15m]))`, refQuery{fn: "rate", r: 15 * time.Minute, by: []string{"l"}, agg: "min"}},
	{`avg by (pod) (bytes_over_time({job="g"} != "@@never@@" | line_format "{{__line__}}" [5m]))`, refQuery{fn: "bytes_over_time", r: 5 * time.Minute, by: []string{"pod"}, agg: "avg"}},
	{`count by (l) (count_over_time({job="g"} != "@@never@@" | line_format "{{__line__}}" [5m] offset 7m))`, refQuery{fn: "count_over_time", r: 5 * time.Minute, offset: 7 * time.Minute, by: []string{"l"}, agg: "count"}},
	{`count_over_time({job="g"} != "@@never@@" | line_format "{{__line__}}" [5m]) > 6`, refQuery{fn: "count_over_time", r: 5 * time.Minute, gt: gt(6)}},
	{`sum by (l) (count_over_time({job="g"} != "@@never@@" | line_format "{{__line__}}" [5m]) > 6)`, refQuery{fn: "count_over_time", r: 5 * time.Minute, by: []string{"l"}, agg: "sum", gt: gt(6)}},
	{`sum by (l) (sum_over_time({job="g"} | logfmt | drop pad | unwrap size [5m]))`, refQuery{fn: "sum_over_time", r: 5 * time.Minute, by: []string{"l"}, agg: "sum"}},
	{`sum_over_time({job="g"} ` + unwrapStages + ` [15m]) by (l)`, refQuery{fn: "sum_over_time", r: 15 * time.Minute, by: []string{"l"}}},
	{`rate({job="g"} ` + unwrapStages + ` [5m]) by (l)`, refQuery{fn: "rate_unwrap", r: 5 * time.Minute, by: []string{"l"}}},
	{`avg_over_time({job="g"} ` + unwrapStages + ` [1h]) by (l)`, refQuery{fn: "avg_over_time", r: time.Hour, by: []string{"l"}}},
	{`min_over_time({job="g"} ` + unwrapStages + ` [5m]) by (l, pod)`, refQuery{fn: "min_over_time", r: 5 * time.Minute, by: []string{"l", "pod"}}},
	{`max_over_time({job="g"} ` + unwrapStages + ` [15m]) by (l)`, refQuery{fn: "max_over_time", r: 15 * time.Minute, by: []string{"l"}}},
	{`first_over_time({job="g"} ` + unwrapStages + ` [5m]) by (l, pod)`, refQuery{fn: "first_over_time", r: 5 * time.Minute, by: []string{"l", "pod"}}},
	{`last_over_time({job="g"} ` + unwrapStages + ` [15m]) by (l, pod)`, refQuery{fn: "last_over_time", r: 15 * time.Minute, by: []string{"l", "pod"}}},
	{`max_over_time({job="g"} ` + unwrapStages + ` [5m] offset 7m) by (l)`, refQuery{fn: "max_over_time", r: 5 * time.Minute, offset: 7 * time.Minute, by: []string{"l"}}},
	{`quantile_over_time(0.9, {job="g"} ` + unwrapStages + ` [5m]) by (l)`, refQuery{fn: "quantile_over_time", r: 5 * time.Minute, q: 0.9, by: []string{"l"}}},
	{`quantile_over_time(0.25, {job="g"} ` + unwrapStages + ` [1h]) by (l, pod)`, refQuery{fn: "quantile_over_time", r: time.Hour, q: 0.25, by: []string{"l", "pod"}}},
}

// gridRequests: aligned and unaligned starts, step <, = and > R, a step not
// dividing R, and instant queries on and off a boundary.
var gridRequests = map[string]request{
	"aligned/60s":     rangeReq(gridD0.Add(2*time.Hour), gridD0.Add(6*time.Hour), time.Minute),
	"aligned/300s":    rangeReq(gridD0.Add(2*time.Hour), gridD0.Add(6*time.Hour), 5*time.Minute),
	"unaligned/300s":  rangeReq(gridD0.Add(2*time.Hour+time.Minute), gridD0.Add(6*time.Hour+time.Minute), 5*time.Minute),
	"unaligned/60s":   rangeReq(gridD0.Add(2*time.Hour+7*time.Second), gridD0.Add(6*time.Hour+7*time.Second), time.Minute),
	"unaligned/900s":  rangeReq(gridD0.Add(2*time.Hour+720*time.Second), gridD0.Add(6*time.Hour+720*time.Second), 15*time.Minute),
	"aligned/3600s":   rangeReq(gridD0.Add(2*time.Hour), gridD0.Add(6*time.Hour), time.Hour),
	"unaligned/3600s": rangeReq(gridD0.Add(2*time.Hour+1234*time.Second), gridD0.Add(6*time.Hour+1234*time.Second), time.Hour),
	"unaligned/420s":  rangeReq(gridD0.Add(2*time.Hour+60*time.Second), gridD0.Add(6*time.Hour), 7*time.Minute),
	"unaligned/7200s": rangeReq(gridD0.Add(1*time.Hour+4020*time.Second), gridD0.Add(7*time.Hour), 2*time.Hour),
	"instant":         instantReq(gridD0.Add(4 * time.Hour)),
	"instant+1m":      instantReq(gridD0.Add(4*time.Hour + time.Minute)),
	"instant+7m":      instantReq(gridD0.Add(4*time.Hour + 7*time.Minute)),
	"instant+1234s":   instantReq(gridD0.Add(4*time.Hour + 1234*time.Second)),
}

// TestGoPathEvaluatesOnTheGrid checks every Go-path query against (T-R, T]
// on the epoch k*step grid and at the instant time.
func TestGoPathEvaluatesOnTheGrid(t *testing.T) {
	lines := gridLines()
	for _, c := range goPathCases {
		for name, req := range gridRequests {
			t.Run(c.query+"@"+name, func(t *testing.T) {
				p := runRequest(t, c.query, req, lines)
				want := reference(lines, c.ref, refGrid(req))
				if d := diffSeries(gotSeries(p), want); len(d) > 0 {
					if len(d) > 6 {
						d = append(d[:6], fmt.Sprintf("... %d more", len(d)-6))
					}
					t.Errorf("%s\n%s", c.query, strings.Join(d, "\n"))
				}
			})
		}
	}
}

// TestGoPathReadRange checks that a Go-path query reads exactly
// (first - R - offset, last - offset] of its grid.
func TestGoPathReadRange(t *testing.T) {
	for _, c := range []struct {
		query     string
		r, offset time.Duration
	}{
		{`count_over_time({job="g"} != "@@never@@" | line_format "{{__line__}}" [5m])`, 5 * time.Minute, 0},
		{`rate({job="g"} | logfmt [1h] offset 7m)`, time.Hour, 7 * time.Minute},
		{`quantile_over_time(0.5, {job="g"} ` + unwrapStages + ` [15m]) by (l)`, 15 * time.Minute, 0},
	} {
		for name, req := range gridRequests {
			p := runRequest(t, c.query, req, nil)
			if len(p.sql) != 1 {
				t.Fatalf("%s@%s: %d statements", c.query, name, len(p.sql))
			}
			from, to, ok := readBounds(p.sql[0])
			if !ok {
				t.Fatalf("%s@%s: no read bounds in %s", c.query, name, p.sql[0])
			}
			times := refGrid(req)
			wantFrom := times[0] - c.r.Nanoseconds() - c.offset.Nanoseconds()
			wantTo := times[len(times)-1] - c.offset.Nanoseconds() + 1
			if from != wantFrom || to != wantTo {
				t.Errorf("%s@%s: reads [%d, %d), want [%d, %d)", c.query, name, from, to, wantFrom, wantTo)
			}
		}
	}
}

type stubProcessor struct {
	entries []shared.LogEntry
	ctx     *shared.PlannerContext
}

func (s *stubProcessor) IsMatrix() bool { return true }

func (s *stubProcessor) Process(ctx *shared.PlannerContext, _ chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	s.ctx = ctx
	out := make(chan []shared.LogEntry, 1)
	out <- s.entries
	close(out)
	return out, nil
}

// TestGridPlannerKeepsNonZeroGridPoints: Main gets the grid; zeros and
// off-grid points are dropped.
func TestGridPlannerKeepsNonZeroGridPoints(t *testing.T) {
	mn := int64(time.Minute)
	stub := &stubProcessor{entries: []shared.LogEntry{
		{Fingerprint: 1, TimestampNS: 10 * mn, Value: 1},
		{Fingerprint: 1, TimestampNS: 15 * mn, Value: 0},
		{Fingerprint: 1, TimestampNS: 17 * mn, Value: 2},
		{Fingerprint: 1, TimestampNS: 20 * mn, Value: 3},
		{Err: io.EOF},
	}}
	g := &GridPlanner{Main: stub, Duration: 5 * time.Minute}
	ctx := &shared.PlannerContext{From: time.Unix(0, 11*mn), To: time.Unix(0, 19*mn), Step: 5 * time.Minute}
	ch, err := g.Process(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	var got []int64
	for entries := range ch {
		for _, e := range entries {
			if e.Err == nil {
				got = append(got, e.TimestampNS/mn)
			}
		}
	}
	if !slices.Equal(got, []int64{10, 20}) {
		t.Errorf("kept minutes %v, want [10 20]", got)
	}
	if want := (shared.EvalGrid{FirstNs: 10 * mn, StepNs: 5 * mn, Points: 3}); stub.ctx.Grid == nil || *stub.ctx.Grid != want {
		t.Errorf("grid %+v, want %+v", stub.ctx.Grid, want)
	}
	if ctx.Grid != nil || ctx.From.UnixNano() != 11*mn {
		t.Errorf("the caller's context changed: %+v", ctx)
	}
}

const goCount = `count_over_time({job="g"} != "@@never@@" | line_format "{{__line__}}" [5m])`

// TestGoPathBinaryJoinsPerT checks a binary of two Go-path operands point by
// point against the difference of their references.
func TestGoPathBinaryJoinsPerT(t *testing.T) {
	lines := gridLines()
	shifted := `count_over_time({job="g"} != "@@never@@" | line_format "{{__line__}}" [5m] offset 5m)`
	for name, req := range gridRequests {
		p := runRequest(t, goCount+" - "+shifted, req, lines)
		a := reference(lines, refQuery{fn: "count_over_time", r: 5 * time.Minute}, refGrid(req))
		b := reference(lines, refQuery{fn: "count_over_time", r: 5 * time.Minute, offset: 5 * time.Minute}, refGrid(req))
		want := map[string]map[int64]float64{}
		for k, pts := range a {
			for ts, v := range pts {
				if w, ok := b[k][ts]; ok && v-w != 0 {
					if want[k] == nil {
						want[k] = map[int64]float64{}
					}
					want[k][ts] = v - w
				}
			}
		}
		if d := diffSeries(gotSeries(p), want); len(d) > 0 {
			t.Errorf("%s: %v", name, d[:min(len(d), 6)])
		}
	}
}

// TestBinaryOperandsShareTheGrid checks that a binary with a grid operand
// runs every operand on its own context with From floored to Step.
func TestBinaryOperandsShareTheGrid(t *testing.T) {
	for _, c := range []struct {
		query  string
		onGrid bool
	}{
		{goCount + ` / count_over_time({job="g"} != "x" [5m])`, true},
		{`count_over_time({job="g"} != "x" [5m]) / (` + goCount + ` * 2)`, true},
		{`absent_over_time({job="g"} [5m]) * count_over_time({job="g"} [5m])`, false},
	} {
		chain, err := Transpile(c.query)
		if err != nil {
			t.Fatal(err)
		}
		bin := chain[0].(*ZeroEaterPlanner).Main.(*BinaryExprProcessor)
		if bin.OnGrid != c.onGrid {
			t.Errorf("%s: OnGrid %v, want %v", c.query, bin.OnGrid, c.onGrid)
		}
	}
	left, right := &stubProcessor{}, &stubProcessor{}
	bin := &BinaryExprProcessor{Left: shared.RequestProcessorChain{left}, Right: shared.RequestProcessorChain{right}, Op: "+", OnGrid: true}
	ctx := &shared.PlannerContext{From: time.Unix(1234, 0), To: time.Unix(4000, 0), Step: 5 * time.Minute}
	ch, err := bin.Process(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for range ch {
	}
	for _, s := range []*stubProcessor{left, right} {
		if s.ctx == ctx || s.ctx.From.Unix() != 1200 {
			t.Errorf("operand context %p From %d, want a copy at 1200", s.ctx, s.ctx.From.Unix())
		}
	}
}

type genProcessor struct {
	gen func(ctx *shared.PlannerContext) []shared.LogEntry
}

func (g *genProcessor) IsMatrix() bool { return true }

func (g *genProcessor) Process(ctx *shared.PlannerContext, _ chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	out := make(chan []shared.LogEntry, 1)
	out <- g.gen(ctx)
	close(out)
	return out, nil
}

// TestMixedBinaryJoinsEveryGridPoint joins a grid operand with an R-bucket
// operand over a request that starts and ends off a step boundary.
func TestMixedBinaryJoinsEveryGridPoint(t *testing.T) {
	r, step := 5*time.Minute, time.Minute
	grid := &GridPlanner{Duration: r, Main: &genProcessor{gen: func(ctx *shared.PlannerContext) []shared.LogEntry {
		var es []shared.LogEntry
		for k := int64(0); k < ctx.Grid.Points; k++ {
			es = append(es, shared.LogEntry{Fingerprint: 1, TimestampNS: ctx.Grid.At(k), Value: 1})
		}
		return es
	}}}
	buckets := &FixPeriodPlanner{Duration: r, Main: &genProcessor{gen: func(ctx *shared.PlannerContext) []shared.LogEntry {
		var es []shared.LogEntry
		for ts := ctx.From; ts.Before(ctx.To); ts = ts.Add(r) {
			es = append(es, shared.LogEntry{Fingerprint: 1, TimestampNS: ts.UnixNano(), Value: 2})
		}
		return es
	}}}
	bin := &BinaryExprProcessor{Left: shared.RequestProcessorChain{grid}, Right: shared.RequestProcessorChain{buckets},
		Op: "+", OnGrid: true}
	req := rangeReq(gridD0.Add(2*time.Hour+7*time.Second), gridD0.Add(3*time.Hour+31*time.Second), step)
	ch, err := bin.Process(&shared.PlannerContext{From: req.start, To: req.end, Step: step}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var got []int64
	for entries := range ch {
		for _, e := range entries {
			if e.Err == nil && e.Value == 3 {
				got = append(got, e.TimestampNS)
			}
		}
	}
	if want := refGrid(req); !slices.Equal(got, want) {
		t.Errorf("joined %d points, want the %d grid points %v..%v", len(got), len(want), want[0], want[len(want)-1])
	}
}
