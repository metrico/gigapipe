package planner

import (
	"math"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
)

type sample struct {
	ts int64
	v  float64
}

// evalWindows buckets samples and returns point index -> value.
func evalWindows(t *testing.T, agg rangeAgg, grid shared.EvalGrid, samples []sample) map[int64]float64 {
	t.Helper()
	ws, err := newWindows(grid, agg.r, agg.offset)
	if err != nil {
		t.Fatal(err)
	}
	s := agg.newSeries(nil, ws.buckets)
	for _, x := range samples {
		if i, ok := ws.bucket(x.ts); ok {
			agg.add(s, i, x.ts, x.v)
		}
	}
	got := map[int64]float64{}
	agg.evaluate(ws, s, func(k int64, v float64) { got[k] = v })
	return got
}

// bruteForce evaluates agg at every point over (T-offset-R, T-offset].
func bruteForce(agg rangeAgg, grid shared.EvalGrid, samples []sample) map[int64]float64 {
	want := map[int64]float64{}
	for k := int64(0); k < grid.Points; k++ {
		hi := grid.At(k) - agg.offset.Nanoseconds()
		lo := hi - agg.r.Nanoseconds()
		var in []sample
		for _, x := range samples {
			if x.ts > lo && x.ts <= hi {
				in = append(in, x)
			}
		}
		if len(in) == 0 {
			continue
		}
		slices.SortStableFunc(in, func(a, b sample) int { return int(a.ts - b.ts) })
		var sum float64
		var vs []float64
		for _, x := range in {
			sum += x.v
			vs = append(vs, x.v)
		}
		n, rs := float64(len(in)), agg.r.Seconds()
		var v float64
		switch agg.fn {
		case "count_over_time":
			v = n
		case "rate":
			v = sum / rs
		case "sum_over_time", "bytes_over_time":
			v = sum
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
			v = quantile(agg.q, vs)
		}
		want[k] = v
	}
	return want
}

func sameValues(a, b map[int64]float64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		w, ok := b[k]
		if !ok || !(v == w || math.Abs(v-w) <= 1e-9*math.Max(1, math.Abs(w)) || (math.IsNaN(v) && math.IsNaN(w))) {
			return false
		}
	}
	return true
}

// TestWindowsSlideMatchesBruteForce: step <, =, > and coprime with R, with and
// without offset, and instant, against a direct evaluation of every window.
func TestWindowsSlideMatchesBruteForce(t *testing.T) {
	const s = int64(time.Second)
	rng := rand.New(rand.NewSource(7))
	var samples []sample
	for ts := int64(0); ts < 6*3600*s; ts += int64(rng.Intn(40000)) * int64(time.Millisecond) {
		samples = append(samples, sample{ts, float64(rng.Intn(100) - 20)})
	}
	for m := int64(0); m < 360; m += 5 {
		samples = append(samples, sample{m * 60 * s, float64(m)})
	}
	grids := map[string]shared.EvalGrid{
		"step60":       {FirstNs: 3600 * s, StepNs: 60 * s, Points: 120},
		"step300":      {FirstNs: 3600 * s, StepNs: 300 * s, Points: 30},
		"step420":      {FirstNs: 3780 * s, StepNs: 420 * s, Points: 20},
		"step3600":     {FirstNs: 3600 * s, StepNs: 3600 * s, Points: 4},
		"step7":        {FirstNs: 3605 * s, StepNs: 7 * s, Points: 400},
		"instant":      {FirstNs: 7200 * s, Points: 1},
		"instant+7.5s": {FirstNs: 7207*s + 500*int64(time.Millisecond), Points: 1},
	}
	fns := []string{"count_over_time", "rate", "sum_over_time", "avg_over_time", "min_over_time",
		"max_over_time", "first_over_time", "last_over_time", "quantile_over_time"}
	for gname, grid := range grids {
		for _, r := range []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute, time.Hour} {
			for _, off := range []time.Duration{0, 7 * time.Minute, 90 * time.Second} {
				for _, fn := range fns {
					agg := rangeAgg{fn: fn, unwrap: fn != "count_over_time", r: r, q: 0.9, offset: off}
					got := evalWindows(t, agg, grid, samples)
					want := bruteForce(agg, grid, samples)
					if len(want) == 0 {
						t.Fatalf("%s %s r=%s off=%s: vacuous", gname, fn, r, off)
					}
					if !sameValues(got, want) {
						t.Errorf("%s %s r=%s off=%s:\n got  %v\n want %v", gname, fn, r, off, got, want)
					}
				}
			}
		}
	}
}

// TestWindowsAreLeftOpenRightClosed puts samples on T, T-R and either side.
func TestWindowsAreLeftOpenRightClosed(t *testing.T) {
	const T = int64(3600 * time.Second)
	r := 5 * time.Minute
	grid := shared.EvalGrid{FirstNs: T, StepNs: int64(r), Points: 2}
	samples := []sample{{T - int64(r), 100}, {T - int64(r) + 1, 1}, {T, 2}, {T + 1, 1000}}
	got := evalWindows(t, rangeAgg{fn: "sum_over_time", unwrap: true, r: r}, grid, samples)
	want := map[int64]float64{0: 3, 1: 1000}
	if !sameValues(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// TestWindowsOffsetReadsThePast checks that offset o evaluates
// (T-o-R, T-o] and never reads after T-o.
func TestWindowsOffsetReadsThePast(t *testing.T) {
	const T = int64(3600 * time.Second)
	r, off := 5*time.Minute, 7*time.Minute
	grid := shared.EvalGrid{FirstNs: T, Points: 1}
	samples := []sample{
		{T - int64(off) - int64(r), 100},
		{T - int64(off) - int64(r) + 1, 1},
		{T - int64(off), 2},
		{T - int64(off) + 1, 1000},
		{T, 10000},
	}
	got := evalWindows(t, rangeAgg{fn: "sum_over_time", unwrap: true, r: r, offset: off}, grid, samples)
	if want := (map[int64]float64{0: 3}); !sameValues(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// TestWindowsQuantilePerT checks that every point takes the quantile of its
// own window's samples.
func TestWindowsQuantilePerT(t *testing.T) {
	const s = int64(time.Second)
	r := 3 * time.Second
	grid := shared.EvalGrid{FirstNs: 3 * s, StepNs: s, Points: 3}
	samples := []sample{{1 * s, 4}, {2 * s, 1}, {3 * s, 7}, {4 * s, 10}, {5 * s, 2}}
	got := evalWindows(t, rangeAgg{fn: "quantile_over_time", unwrap: true, q: 0.5, r: r}, grid, samples)
	// (0s,3s] = {4,1,7}; (1s,4s] = {1,7,10}; (2s,5s] = {7,10,2}
	if want := (map[int64]float64{0: 4, 1: 7, 2: 7}); !sameValues(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	got = evalWindows(t, rangeAgg{fn: "quantile_over_time", unwrap: true, q: 0.25, r: r}, grid, samples)
	if want := (map[int64]float64{0: 2.5, 1: 4, 2: 4.5}); !sameValues(got, want) {
		t.Errorf("q=0.25: got %v, want %v", got, want)
	}
}

// TestWindowsNonFiniteSumStaysInItsWindow checks that an infinite value only
// affects the windows that hold it.
func TestWindowsNonFiniteSumStaysInItsWindow(t *testing.T) {
	const s = int64(time.Second)
	r := 2 * time.Second
	grid := shared.EvalGrid{FirstNs: 2 * s, StepNs: s, Points: 5}
	samples := []sample{{1 * s, 1}, {2 * s, math.Inf(1)}, {3 * s, math.Inf(-1)}, {5 * s, 2}, {6 * s, 3}}
	got := evalWindows(t, rangeAgg{fn: "sum_over_time", unwrap: true, r: r}, grid, samples)
	want := map[int64]float64{0: math.Inf(1), 1: math.NaN(), 2: math.Inf(-1), 3: 2, 4: 5}
	if !sameValues(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestWindowsRejectUnsupportedFunctions(t *testing.T) {
	for _, agg := range []rangeAgg{
		{fn: "stddev_over_time", unwrap: true},
		{fn: "stdvar_over_time", unwrap: true},
		{fn: "sum_over_time"},
	} {
		if agg.check() == nil {
			t.Errorf("%+v: accepted", agg)
		}
	}
}
