package planner

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
)

// maxWindowBuckets bounds the buckets of one series.
const maxWindowBuckets = 10_000_000

// windows splits (first-R, last] into w = gcd(step, R) wide buckets, left-open,
// keyed on ts + offset; the window (T-R, T] of every point is whole buckets.
type windows struct {
	grid     shared.EvalGrid
	base     int64
	w        int64
	perStep  int64
	perRange int64
	buckets  int64
	offsetNs int64
}

func newWindows(grid shared.EvalGrid, r, offset time.Duration) (windows, error) {
	rn := r.Nanoseconds()
	if rn <= 0 {
		return windows{}, fmt.Errorf("range must be positive, got %s", r)
	}
	w := shared.Gcd(grid.StepNs, rn)
	ws := windows{
		grid:     grid,
		base:     grid.FirstNs - rn,
		w:        w,
		perStep:  grid.StepNs / w,
		perRange: rn / w,
		offsetNs: offset.Nanoseconds(),
	}
	ws.buckets = (grid.Points-1)*ws.perStep + ws.perRange
	if ws.buckets > maxWindowBuckets {
		return windows{}, errStreamTooLong
	}
	return ws, nil
}

// bucket returns the bucket of a sample read at ts.
func (ws windows) bucket(ts int64) (int64, bool) {
	d := ts + ws.offsetNs - ws.base
	if d <= 0 {
		return 0, false
	}
	i := (d - 1) / ws.w
	return i, i < ws.buckets
}

// span returns the buckets [lo, hi) of the window of point k.
func (ws windows) span(k int64) (int64, int64) {
	lo := k * ws.perStep
	return lo, lo + ws.perRange
}

// rangeAgg is a range aggregation evaluated per window.
type rangeAgg struct {
	fn     string
	unwrap bool
	q      float64
	r      time.Duration
	offset time.Duration
}

func (a rangeAgg) check() error {
	switch a.fn {
	case "count_over_time", "rate", "bytes_over_time", "bytes_rate":
		return nil
	case "sum_over_time", "avg_over_time", "min_over_time", "max_over_time",
		"first_over_time", "last_over_time", "quantile_over_time":
		if a.unwrap {
			return nil
		}
	}
	return &shared.NotSupportedError{Msg: fmt.Sprintf("%s is not supported for this query", a.fn)}
}

// sample returns the value an entry adds to its bucket.
func (a rangeAgg) sample(e *shared.LogEntry) float64 {
	switch {
	case a.unwrap:
		return e.Value
	case a.fn == "bytes_over_time" || a.fn == "bytes_rate":
		return float64(len(e.Message))
	}
	return 1
}

// windowSeries holds the buckets of one output series. Only the slices the
// function reads are allocated.
type windowSeries struct {
	labels  map[string]string
	n       []float64
	sum     []float64
	ext     []float64
	first   []float64
	last    []float64
	firstTs []int64
	lastTs  []int64
	values  [][]float64
}

func (a rangeAgg) newSeries(labels map[string]string, buckets int64) *windowSeries {
	s := &windowSeries{labels: labels, n: make([]float64, buckets)}
	switch a.fn {
	case "count_over_time":
	case "rate":
		if a.unwrap {
			s.sum = make([]float64, buckets)
		}
	case "min_over_time", "max_over_time":
		s.ext = make([]float64, buckets)
	case "first_over_time", "last_over_time":
		s.first, s.last = make([]float64, buckets), make([]float64, buckets)
		s.firstTs, s.lastTs = make([]int64, buckets), make([]int64, buckets)
	case "quantile_over_time":
		s.values = make([][]float64, buckets)
	default:
		s.sum = make([]float64, buckets)
	}
	return s
}

func (a rangeAgg) add(s *windowSeries, i, ts int64, v float64) {
	empty := s.n[i] == 0
	s.n[i]++
	switch {
	case s.sum != nil:
		s.sum[i] += v
	case s.ext != nil:
		if empty || (a.fn == "max_over_time" && v > s.ext[i]) || (a.fn == "min_over_time" && v < s.ext[i]) {
			s.ext[i] = v
		}
	case s.first != nil:
		if empty || ts < s.firstTs[i] {
			s.first[i], s.firstTs[i] = v, ts
		}
		if empty || ts >= s.lastTs[i] {
			s.last[i], s.lastTs[i] = v, ts
		}
	case s.values != nil:
		s.values[i] = append(s.values[i], v)
	}
}

// evaluate returns (k, value) for every grid point whose window holds a
// sample, sliding over the buckets once.
func (a rangeAgg) evaluate(ws windows, s *windowSeries, emit func(k int64, v float64)) {
	cumN := prefixSum(s.n)
	var sums *sumPrefix
	if s.sum != nil {
		sums = newSumPrefix(s.sum)
	}
	var ext *extremes
	if s.ext != nil {
		ext = &extremes{vals: s.ext, n: s.n, max: a.fn == "max_over_time"}
	}
	var next, prev []int64
	if s.first != nil {
		next, prev = nonEmpty(s.n)
	}
	var buf []float64
	rs := a.r.Seconds()
	for k := int64(0); k < ws.grid.Points; k++ {
		lo, hi := ws.span(k)
		n := cumN[hi] - cumN[lo]
		if n == 0 {
			continue
		}
		var v float64
		switch a.fn {
		case "count_over_time":
			v = n
		case "rate":
			if a.unwrap {
				v = sums.window(lo, hi) / rs
			} else {
				v = n / rs
			}
		case "bytes_over_time", "sum_over_time":
			v = sums.window(lo, hi)
		case "bytes_rate":
			v = sums.window(lo, hi) / rs
		case "avg_over_time":
			v = sums.window(lo, hi) / n
		case "min_over_time", "max_over_time":
			v = ext.window(lo, hi)
		case "first_over_time":
			v = s.first[next[lo]]
		case "last_over_time":
			v = s.last[prev[hi-1]]
		case "quantile_over_time":
			buf = buf[:0]
			for i := lo; i < hi; i++ {
				buf = append(buf, s.values[i]...)
			}
			v = quantile(a.q, buf)
		}
		emit(k, v)
	}
}

func prefixSum(xs []float64) []float64 {
	cum := make([]float64, len(xs)+1)
	for i, x := range xs {
		cum[i+1] = cum[i] + x
	}
	return cum
}

// sumPrefix sums a run of buckets: prefix sums of the finite bucket sums,
// prefix counts of the +Inf, -Inf and NaN ones.
type sumPrefix struct {
	finite        []float64
	pos, neg, nan []int64
}

func newSumPrefix(xs []float64) *sumPrefix {
	p := &sumPrefix{finite: make([]float64, len(xs)+1)}
	var counts bool
	for _, x := range xs {
		if math.IsInf(x, 0) || math.IsNaN(x) {
			counts = true
			break
		}
	}
	if counts {
		p.pos, p.neg, p.nan = make([]int64, len(xs)+1), make([]int64, len(xs)+1), make([]int64, len(xs)+1)
	}
	for i, x := range xs {
		p.finite[i+1] = p.finite[i]
		if counts {
			p.pos[i+1], p.neg[i+1], p.nan[i+1] = p.pos[i], p.neg[i], p.nan[i]
		}
		switch {
		case math.IsNaN(x):
			p.nan[i+1]++
		case math.IsInf(x, 1):
			p.pos[i+1]++
		case math.IsInf(x, -1):
			p.neg[i+1]++
		default:
			p.finite[i+1] += x
		}
	}
	return p
}

func (p *sumPrefix) window(lo, hi int64) float64 {
	if p.pos != nil {
		pos, neg := p.pos[hi]-p.pos[lo], p.neg[hi]-p.neg[lo]
		switch {
		case p.nan[hi]-p.nan[lo] > 0 || (pos > 0 && neg > 0):
			return math.NaN()
		case pos > 0:
			return math.Inf(1)
		case neg > 0:
			return math.Inf(-1)
		}
	}
	return p.finite[hi] - p.finite[lo]
}

// extremes answers min or max over windows whose bounds never move back,
// with a monotonic deque of non-empty buckets.
type extremes struct {
	vals, n []float64
	max     bool
	deque   []int64
	pushed  int64
}

func (e *extremes) window(lo, hi int64) float64 {
	for ; e.pushed < hi; e.pushed++ {
		i := e.pushed
		if e.n[i] == 0 {
			continue
		}
		for len(e.deque) > 0 && e.dominates(e.vals[i], e.vals[e.deque[len(e.deque)-1]]) {
			e.deque = e.deque[:len(e.deque)-1]
		}
		e.deque = append(e.deque, i)
	}
	for e.deque[0] < lo {
		e.deque = e.deque[1:]
	}
	return e.vals[e.deque[0]]
}

func (e *extremes) dominates(a, b float64) bool {
	if e.max {
		return a >= b
	}
	return a <= b
}

// nonEmpty returns, for every bucket, the nearest non-empty bucket at or
// after it (len(n) if none) and at or before it (-1 if none).
func nonEmpty(n []float64) ([]int64, []int64) {
	next, prev := make([]int64, len(n)), make([]int64, len(n))
	last := int64(-1)
	for i := range n {
		if n[i] > 0 {
			last = int64(i)
		}
		prev[i] = last
	}
	last = int64(len(n))
	for i := len(n) - 1; i >= 0; i-- {
		if n[i] > 0 {
			last = int64(i)
		}
		next[i] = last
	}
	return next, prev
}

// quantile interpolates linearly between the closest ranks.
func quantile(q float64, vs []float64) float64 {
	switch {
	case len(vs) == 0 || math.IsNaN(q):
		return math.NaN()
	case q < 0:
		return math.Inf(-1)
	case q > 1:
		return math.Inf(1)
	}
	sort.Float64s(vs)
	rank := q * float64(len(vs)-1)
	lo := math.Max(0, math.Floor(rank))
	hi := math.Min(float64(len(vs)-1), lo+1)
	w := rank - math.Floor(rank)
	return vs[int(lo)]*(1-w) + vs[int(hi)]*w
}

// processWindows evaluates agg at every point of ctx.Grid, one output series
// per input fingerprint.
func (p *AggregatorPlanner) processWindows(ctx *shared.PlannerContext, in chan []shared.LogEntry,
	agg rangeAgg) (chan []shared.LogEntry, error) {
	if ctx.Grid == nil {
		return nil, fmt.Errorf("%s: no evaluation grid", agg.fn)
	}
	if err := agg.check(); err != nil {
		return nil, err
	}
	ws, err := newWindows(*ctx.Grid, agg.r, agg.offset)
	if err != nil {
		return nil, err
	}
	series := map[uint64]*windowSeries{}
	return p.GenericPlanner.WrapProcess(ctx, in, GenericPlannerOps{
		OnEntry: func(e *shared.LogEntry) error {
			if e.Err != nil {
				return e.Err
			}
			i, ok := ws.bucket(e.TimestampNS)
			if !ok {
				return nil
			}
			s := series[e.Fingerprint]
			if s == nil {
				if len(series) >= maxSeries {
					return errTooManySeries
				}
				s = agg.newSeries(e.Labels, ws.buckets)
				series[e.Fingerprint] = s
			}
			agg.add(s, i, e.TimestampNS, agg.sample(e))
			return nil
		},
		OnAfterEntriesSlice: func(entries []shared.LogEntry, c chan []shared.LogEntry) error {
			return nil
		},
		OnAfterEntries: func(out chan []shared.LogEntry) error {
			for fp, s := range series {
				var entries []shared.LogEntry
				agg.evaluate(ws, s, func(k int64, v float64) {
					entries = append(entries, shared.LogEntry{
						Fingerprint: fp,
						TimestampNS: ws.grid.At(k),
						Labels:      s.labels,
						Value:       v,
					})
				})
				if len(entries) > 0 {
					out <- entries
				}
			}
			return nil
		},
	})
}
