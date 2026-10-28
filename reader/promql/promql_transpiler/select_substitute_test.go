package promql_transpiler

import (
	"fmt"
	"regexp"
	"testing"
)

// subRead is a grid-keyed substitute read recovered from its SQL.
type subRead struct {
	// Keys are intDiv(t-Phase+Width-1, Width)*Width+Phase.
	Phase, Width int64
	// Fill rows are FillStep apart.
	FillStep int64
	// Rows are kept on GridPhase + k*GridStep.
	GridPhase, GridStep int64
}

var (
	reSubKey   = regexp.MustCompile(`intDiv\((?:intDiv\(samples\.timestamp_ns, 1000000\)|ts_ms)(?: - (\d+))? \+ (\d+), (\d+)\) \* (\d+)(?: \+ (\d+))? as timestamp_ms`)
	reSubStale = regexp.MustCompile(`WITH FILL TO \d+ STEP (\d+) STALENESS \d+`)
	reSubArray = regexp.MustCompile(`arrayJoin\(range\(bucket_ms, least\(bucket_ms \+ toInt64\(\d+\), next_ms, toInt64\(\d+\)\), toInt64\((\d+)\)\)\)`)
	reSubGrid  = regexp.MustCompile(`\(\(\(?timestamp_ms(?: - (\d+)\))? % (\d+)\) == \(0\)\)`)
)

func parseSubRead(t *testing.T, s string, staleness bool) subRead {
	t.Helper()
	var r subRead
	keys := reSubKey.FindAllStringSubmatch(s, -1)
	if len(keys) == 0 {
		t.Fatalf("no bucket key in %s", s)
	}
	for _, k := range keys {
		if k[1] != keys[0][1] || k[3] != keys[0][3] {
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
	fill, other := reSubArray, reSubStale
	if staleness {
		fill, other = reSubStale, reSubArray
	}
	m := fill.FindStringSubmatch(s)
	if m == nil || other.MatchString(s) {
		t.Fatalf("staleness=%v: wrong fill in %s", staleness, s)
	}
	r.FillStep = atoi(t, m[1])
	g := reSubGrid.FindStringSubmatch(s)
	if g == nil {
		t.Fatalf("no evaluation point filter in %s", s)
	}
	if g[1] != "" {
		r.GridPhase = atoi(t, g[1])
	}
	r.GridStep = atoi(t, g[2])
	return r
}

var substituteGridQueries = []struct {
	query   string
	shiftMs int64
}{
	{"rate(gc[15m])", 0},
	{"rate(gc[1h])", 0},
	{"increase(gc[1h] offset 7m)", 420_000},
	{"changes(gx[15m])", 0},
	{"sum by (l) (gx)", 0},
	{"sum(rate(gc[15m]))", 0},
	{"avg_over_time(gx[1h])", 0},
	{"max_over_time(gx[5m])", 0},
}

// Every evaluation point of a substitute is a bucket key, its fill steps by
// the bucket width on both fill paths, and it returns only the rows on its
// grid. A substitute whose buckets would be 15s wide is not made.
func TestTranspileSelectSubstituteKeysAreEvaluationPoints(t *testing.T) {
	for _, staleness := range []bool{false, true} {
		for _, g := range latticeGrids() {
			for _, q := range substituteGridQueries {
				t.Run(fmt.Sprintf("staleness=%v %s %s", staleness, g.name, q.query), func(t *testing.T) {
					plans := planSelects(t, q.query, g.eval, planOpts{tag: true, metrics15s: true, staleness: staleness})
					subs := 0
					for _, p := range plans {
						if p.Route != RouteSubstitute {
							continue
						}
						subs++
						r := parseSubRead(t, p.SQL, staleness)
						if r.FillStep != r.Width || r.GridStep%r.Width != 0 {
							t.Fatalf("%+v: fill or grid step is not a multiple of the width", r)
						}
						for _, T := range evalPoints(g.eval, "gx") {
							T -= q.shiftMs
							if (T-r.Phase)%r.Width != 0 || (T-r.GridPhase)%r.GridStep != 0 {
								t.Fatalf("point %d is not a key of %+v", T, r)
							}
						}
					}
					if wantSub := g.eval.StepMs != 15_000; (subs > 0) != wantSub {
						t.Fatalf("%d substitute reads, want substitute %v: %+v", subs, wantSub, routes(plans))
					}
				})
			}
		}
	}
}

// Instant substitutes key their single bucket on the point, one lookback or
// range wide.
func TestTranspileSelectSubstituteSinglePointGrid(t *testing.T) {
	T := selectStartMs + 3_600_000 + 45_000
	cases := []struct {
		query string
		width int64
	}{
		{"sum(gx)", 300_000},
		{"sum by (l) (gx)", 300_000},
		{"rate(gc[1h])", 3_600_000},
		{"sum(rate(gc[15m]))", 900_000},
		{"max_over_time(gx[5m])", 300_000},
	}
	for _, c := range cases {
		plans := planSelects(t, c.query, EvalGrid{StartMs: T, EndMs: T}, planOpts{tag: true, metrics15s: true})
		if len(plans) != 1 || plans[0].Route != RouteSubstitute {
			t.Fatalf("%s: %+v", c.query, routes(plans))
		}
		r := parseSubRead(t, plans[0].SQL, false)
		want := subRead{Phase: T % c.width, Width: c.width, FillStep: c.width, GridPhase: T % c.width, GridStep: c.width}
		if r != want {
			t.Errorf("%s: %+v, want %+v", c.query, r, want)
		}
	}
}
