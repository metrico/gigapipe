package logql_transpiler

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

const (
	rawSel     = `{job="g"} != "@@never@@"`
	rawUnwrap  = `| regexp "size=(?P<size>[0-9]+)" | unwrap size`
	rawCount5m = `count_over_time(` + rawSel + ` [5m])`
)

// rawSQLCases are served by the raw SQL planners: a non-empty line filter or
// a regexp parser keeps them off the shortcut and the Go path.
var rawSQLCases = []struct {
	query string
	ref   refQuery
}{
	{rawCount5m, refQuery{fn: "count_over_time", r: 5 * time.Minute}},
	{`rate(` + rawSel + ` [15m])`, refQuery{fn: "rate", r: 15 * time.Minute}},
	// bytes_over_time is checked as bytes_rate on this path.
	{`bytes_over_time(` + rawSel + ` [5m])`, refQuery{fn: "bytes_rate", r: 5 * time.Minute}},
	{`bytes_rate(` + rawSel + ` [1h])`, refQuery{fn: "bytes_rate", r: time.Hour}},
	{`sum by (l) (count_over_time(` + rawSel + ` [5m]))`, refQuery{fn: "count_over_time", r: 5 * time.Minute, by: []string{"l"}, agg: "sum"}},
	{`sum by (l) (rate(` + rawSel + ` [1h]))`, refQuery{fn: "rate", r: time.Hour, by: []string{"l"}, agg: "sum"}},
	{`count_over_time(` + rawSel + ` [5m] offset 7m)`, refQuery{fn: "count_over_time", r: 5 * time.Minute, offset: 7 * time.Minute}},
	{`sum by (l) (count_over_time(` + rawSel + ` [15m] offset 1h))`, refQuery{fn: "count_over_time", r: 15 * time.Minute, offset: time.Hour, by: []string{"l"}, agg: "sum"}},
	{`max by (l) (count_over_time(` + rawSel + ` [5m]))`, refQuery{fn: "count_over_time", r: 5 * time.Minute, by: []string{"l"}, agg: "max"}},
	{`min by (l) (rate(` + rawSel + ` [15m]))`, refQuery{fn: "rate", r: 15 * time.Minute, by: []string{"l"}, agg: "min"}},
	{`count by (l) (count_over_time(` + rawSel + ` [5m] offset 7m))`, refQuery{fn: "count_over_time", r: 5 * time.Minute, offset: 7 * time.Minute, by: []string{"l"}, agg: "count"}},
	{rawCount5m + ` > 6`, refQuery{fn: "count_over_time", r: 5 * time.Minute, gt: gt(6)}},
	{`sum by (l) (` + rawCount5m + ` > 6)`, refQuery{fn: "count_over_time", r: 5 * time.Minute, by: []string{"l"}, agg: "sum", gt: gt(6)}},
	{`sum by (l) (sum_over_time({job="g"} ` + rawUnwrap + ` [5m]))`, refQuery{fn: "sum_over_time", r: 5 * time.Minute, by: []string{"l"}, agg: "sum"}},
	{`sum_over_time({job="g"} ` + rawUnwrap + ` [15m]) by (l)`, refQuery{fn: "sum_over_time", r: 15 * time.Minute, by: []string{"l"}}},
	{`rate({job="g"} ` + rawUnwrap + ` [5m]) by (l)`, refQuery{fn: "rate_unwrap", r: 5 * time.Minute, by: []string{"l"}}},
	{`avg_over_time({job="g"} ` + rawUnwrap + ` [1h]) by (l)`, refQuery{fn: "avg_over_time", r: time.Hour, by: []string{"l"}}},
	{`min_over_time({job="g"} ` + rawUnwrap + ` [5m]) by (l, pod)`, refQuery{fn: "min_over_time", r: 5 * time.Minute, by: []string{"l", "pod"}}},
	{`max_over_time({job="g"} ` + rawUnwrap + ` [15m]) by (l)`, refQuery{fn: "max_over_time", r: 15 * time.Minute, by: []string{"l"}}},
	{`first_over_time({job="g"} ` + rawUnwrap + ` [5m]) by (l, pod)`, refQuery{fn: "first_over_time", r: 5 * time.Minute, by: []string{"l", "pod"}}},
	{`last_over_time({job="g"} ` + rawUnwrap + ` [15m]) by (l, pod)`, refQuery{fn: "last_over_time", r: 15 * time.Minute, by: []string{"l", "pod"}}},
	{`max_over_time({job="g"} ` + rawUnwrap + ` [5m] offset 7m) by (l)`, refQuery{fn: "max_over_time", r: 5 * time.Minute, offset: 7 * time.Minute, by: []string{"l"}}},
	{`quantile_over_time(0.9, {job="g"} ` + rawUnwrap + ` [5m]) by (l)`, refQuery{fn: "quantile_over_time", r: 5 * time.Minute, q: 0.9, by: []string{"l"}}},
	{`quantile_over_time(0.25, {job="g"} ` + rawUnwrap + ` [1h]) by (l, pod)`, refQuery{fn: "quantile_over_time", r: time.Hour, q: 0.25, by: []string{"l", "pod"}}},
	{`stdvar_over_time({job="g"} ` + rawUnwrap + ` [15m]) by (l)`, refQuery{fn: "stdvar_over_time", r: 15 * time.Minute, by: []string{"l"}}},
	{`stddev_over_time({job="g"} ` + rawUnwrap + ` [5m] offset 7m) by (l, pod)`, refQuery{fn: "stddev_over_time", r: 5 * time.Minute, offset: 7 * time.Minute, by: []string{"l", "pod"}}},
}

// TestRawSQLPlansOnTheGrid checks that every raw SQL query runs under the
// grid emitter and reads exactly (first - R - offset, last - offset].
func TestRawSQLPlansOnTheGrid(t *testing.T) {
	for _, c := range rawSQLCases {
		for name, req := range gridRequests {
			p := runRequest(t, c.query, req, nil)
			if _, ok := p.root.(*GridPlanner); !ok {
				t.Fatalf("%s@%s: root %T, want *GridPlanner", c.query, name, p.root)
			}
			if len(p.sql) != 1 {
				t.Fatalf("%s@%s: %d statements", c.query, name, len(p.sql))
			}
			from, to, ok := readBounds(p.sql[0])
			if !ok {
				t.Fatalf("%s@%s: no read bounds in %s", c.query, name, p.sql[0])
			}
			times := refGrid(req)
			wantFrom := times[0] - c.ref.r.Nanoseconds() - c.ref.offset.Nanoseconds() + 1
			wantTo := times[len(times)-1] - c.ref.offset.Nanoseconds() + 1
			if from != wantFrom || to != wantTo {
				t.Errorf("%s@%s: reads [%d, %d), want [%d, %d)", c.query, name, from, to, wantFrom, wantTo)
			}
			if strings.Contains(p.sql[0], fmt.Sprintf("timestamp_ns, %d) * %[1]d", c.ref.r.Nanoseconds())) {
				t.Errorf("%s@%s: floors to R: %s", c.query, name, p.sql[0])
			}
		}
	}
}

// TestRawSQLRoutesOnTheGrid covers raw SQL shapes without a value check.
func TestRawSQLRoutesOnTheGrid(t *testing.T) {
	for _, q := range []string{
		`sum by (a) (count_over_time({job="g"} | json a="x" [5m]))`,
		`topk(1, sum by (l) (` + rawCount5m + `))`,
		`sum by (l) (rate(` + rawSel + ` |= "size" [5m]))`,
	} {
		for name, req := range gridRequests {
			if p := runRequest(t, q, req, nil); !isGrid(p.root) {
				t.Errorf("%s@%s: root %T", q, name, p.root)
			}
		}
	}
}

// TestRawSQLWindowShapes pins each window's shape: cumulative difference,
// State fan-out and Merge, offset shift, and one read for an instant query.
func TestRawSQLWindowShapes(t *testing.T) {
	const cumulative = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
	at := gridD0.Add(4 * time.Hour)
	instant := instantReq(at)
	rng := rangeReq(gridD0.Add(2*time.Hour), gridD0.Add(6*time.Hour), 7*time.Minute)
	bucket := func(w time.Duration) string {
		return fmt.Sprintf("intDiv(timestamp_ns - 1, %d) * %[1]d + %[1]d", w.Nanoseconds())
	}
	for _, c := range []struct {
		query    string
		req      request
		has, not []string
	}{
		{rawCount5m, rng, []string{cumulative, bucket(time.Minute)}, []string{"State(", "Merge("}},
		{`rate({job="g"} ` + rawUnwrap + ` [15m]) by (l)`, rng,
			[]string{cumulative, bucket(time.Minute), "isFinite(value)"}, []string{"State("}},
		{`avg_over_time({job="g"} ` + rawUnwrap + ` [1h]) by (l)`, rng, []string{cumulative}, []string{"State("}},
		{`max_over_time({job="g"} ` + rawUnwrap + ` [5m]) by (l)`, rng,
			[]string{"maxState(value)", "maxMerge(st)", "arrayJoin(range(", bucket(time.Minute)}, []string{" OVER "}},
		{`quantile_over_time(0.9, {job="g"} ` + rawUnwrap + ` [5m]) by (l)`, rng,
			[]string{"quantileState(0.9)(value)", "quantileMerge(0.9)(st)"}, []string{" OVER "}},
		{`first_over_time({job="g"} ` + rawUnwrap + ` [5m]) by (l)`, rng,
			[]string{"argMinState(value, timestamp_ns)", "argMinMerge(st)"}, []string{" OVER "}},
		{`count_over_time(` + rawSel + ` [5m] offset 7m)`, rng,
			[]string{"samples.timestamp_ns + 420000000000 as timestamp_ns", cumulative}, nil},
		{`quantile_over_time(0.9, {job="g"} ` + rawUnwrap + ` [5m]) by (l)`, instant,
			[]string{"quantile(0.9)(value)"}, []string{" OVER ", "arrayJoin(range(", "State("}},
		{rawCount5m, instant, []string{
			fmt.Sprintf("(samples.timestamp_ns) >= (%d)", at.Add(-5*time.Minute).UnixNano()+1),
			fmt.Sprintf("(samples.timestamp_ns) < (%d)", at.UnixNano()+1),
		}, []string{" OVER ", "arrayJoin(range(", "State("}},
		{`max_over_time({job="g"} ` + rawUnwrap + ` [5m] offset 7m) by (l)`, instant, []string{
			"samples.timestamp_ns + 420000000000 as timestamp_ns",
			fmt.Sprintf("(samples.timestamp_ns) >= (%d)", at.Add(-12*time.Minute).UnixNano()+1),
			fmt.Sprintf("(samples.timestamp_ns) < (%d)", at.Add(-7*time.Minute).UnixNano()+1),
			"max(value)",
		}, []string{" OVER ", "arrayJoin(range(", "State("}},
	} {
		p := runRequest(t, c.query, c.req, nil)
		for _, h := range c.has {
			if !strings.Contains(p.sql[0], h) {
				t.Errorf("%s: no %q in %s", c.query, h, p.sql[0])
			}
		}
		for _, n := range c.not {
			if strings.Contains(p.sql[0], n) {
				t.Errorf("%s: %q in %s", c.query, n, p.sql[0])
			}
		}
	}
}

// TestRawSQLBinaryRouting: a binary of raw SQL operands stays one SQL query
// on the grid; one that mixes them with shortcut operands joins in memory.
func TestRawSQLBinaryRouting(t *testing.T) {
	req := gridRequests["unaligned/300s"]
	for _, q := range []string{
		rawCount5m + ` * count_over_time(` + rawSel + ` [5m] offset 5m)`,
		`sum(rate(` + rawSel + ` [5m])) * 2`,
		`(` + rawCount5m + ` + ` + rawCount5m + `) / 2`,
	} {
		p := runRequest(t, q, req, nil)
		if !isGrid(p.root) || len(p.sql) != 1 {
			t.Errorf("%s: root %T with %d statements, want *GridPlanner with 1", q, p.root, len(p.sql))
		}
	}
	for _, q := range []string{
		rawCount5m + ` / count_over_time({job="g"} [5m])`,
		`count_over_time({job="g"} [5m]) - (` + rawCount5m + ` * 2)`,
	} {
		chain, err := Transpile(q)
		if err != nil {
			t.Fatal(err)
		}
		var bin *BinaryExprProcessor
		zero, ok := chain[0].(*ZeroEaterPlanner)
		if ok {
			bin, ok = zero.Main.(*BinaryExprProcessor)
		}
		if !ok || !bin.OnGrid {
			t.Errorf("%s: root %T, want an in-memory binary on the grid", q, chain[0])
		}
	}
}

// TestRawSQLBinaryOperandsReadTheirWindows: each operand of an SQL binary
// reads its own range and offset.
func TestRawSQLBinaryOperandsReadTheirWindows(t *testing.T) {
	req := gridRequests["aligned/300s"]
	q := `count_over_time(` + rawSel + ` [1h]) * count_over_time(` + rawSel + ` [5m] offset 2h)`
	p := runRequest(t, q, req, nil)
	times := refGrid(req)
	first, last := times[0], times[len(times)-1]
	var got []string
	for _, s := range p.sql {
		froms, tos := reFromNs.FindAllStringSubmatch(s, -1), reToNs.FindAllStringSubmatch(s, -1)
		for i := range froms {
			got = append(got, froms[i][1]+","+tos[i][1])
		}
	}
	slices.Sort(got)
	got = slices.Compact(got)
	want := []string{
		fmt.Sprintf("%d,%d", first-time.Hour.Nanoseconds()+1, last+1),
		fmt.Sprintf("%d,%d", first-(5*time.Minute+2*time.Hour).Nanoseconds()+1, last-(2*time.Hour).Nanoseconds()+1),
	}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("reads %v, want %v", got, want)
	}
}

func isGrid(p any) bool {
	_, ok := p.(*GridPlanner)
	return ok
}
