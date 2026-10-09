package logql_transpiler

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"
)

// shortcutCases are served by the metrics_15s shortcut when the grid, R and
// offset lie on the 15s lattice, and by the raw SQL planners otherwise.
var shortcutCases = []struct {
	query string
	ref   refQuery
}{
	{`count_over_time({job="g"} [5m])`, refQuery{fn: "count_over_time", r: 5 * time.Minute}},
	{`rate({job="g"} [15m])`, refQuery{fn: "rate", r: 15 * time.Minute}},
	{`count_over_time({job="g"} |= "" [1h])`, refQuery{fn: "count_over_time", r: time.Hour}},
	{`rate({job="g"} [45s])`, refQuery{fn: "rate", r: 45 * time.Second}},
	{`count_over_time({job="g"} | line_format "{{ range $k, $v := __line__ }}{{ $k }}{{ end }}" [5m])`, refQuery{fn: "count_over_time", r: 5 * time.Minute}},
	{`sum(count_over_time({job="g"} [90s]))`, refQuery{fn: "count_over_time", r: 90 * time.Second, by: []string{}, agg: "sum"}},
	{`sum by (l) (count_over_time({job="g"} [5m]))`, refQuery{fn: "count_over_time", r: 5 * time.Minute, by: []string{"l"}, agg: "sum"}},
	{`sum by (l) (rate({job="g"} [1h]))`, refQuery{fn: "rate", r: time.Hour, by: []string{"l"}, agg: "sum"}},
	{`count_over_time({job="g"} [5m] offset 7m)`, refQuery{fn: "count_over_time", r: 5 * time.Minute, offset: 7 * time.Minute}},
	{`sum by (l) (count_over_time({job="g"} [15m] offset 1h))`, refQuery{fn: "count_over_time", r: 15 * time.Minute, offset: time.Hour, by: []string{"l"}, agg: "sum"}},
	{`max by (l) (count_over_time({job="g"} [5m]))`, refQuery{fn: "count_over_time", r: 5 * time.Minute, by: []string{"l"}, agg: "max"}},
	{`min by (l) (rate({job="g"} [15m]))`, refQuery{fn: "rate", r: 15 * time.Minute, by: []string{"l"}, agg: "min"}},
	{`count by (l) (count_over_time({job="g"} [5m] offset 7m))`, refQuery{fn: "count_over_time", r: 5 * time.Minute, offset: 7 * time.Minute, by: []string{"l"}, agg: "count"}},
	{`count by (l) (count_over_time({job="g"} [30s]))`, refQuery{fn: "count_over_time", r: 30 * time.Second, by: []string{"l"}, agg: "count"}},
	{`count_over_time({job="g"} [5m]) > 6`, refQuery{fn: "count_over_time", r: 5 * time.Minute, gt: gt(6)}},
	{`sum by (l) (count_over_time({job="g"} [5m]) > 6)`, refQuery{fn: "count_over_time", r: 5 * time.Minute, by: []string{"l"}, agg: "sum", gt: gt(6)}},
}

// offLatticeRequests put an evaluation time off the 15s lattice.
var offLatticeRequests = map[string]request{
	"instant+1234s": instantReq(gridD0.Add(4*time.Hour + 1234*time.Second)),
	"instant+7s":    instantReq(gridD0.Add(4*time.Hour + 7*time.Second)),
	"range/20s":     rangeReq(gridD0.Add(2*time.Hour), gridD0.Add(3*time.Hour), 20*time.Second),
	"range/7s":      rangeReq(gridD0.Add(2*time.Hour+3*time.Second), gridD0.Add(2*time.Hour+30*time.Minute), 7*time.Second),
}

// onLattice reports whether every evaluation time of req is a multiple of 15s.
func onLattice(req request) bool {
	for _, ts := range refGrid(req) {
		if ts%(15*time.Second).Nanoseconds() != 0 {
			return false
		}
	}
	return true
}

var reMetrics15s = regexp.MustCompile(`FROM metrics_15s(_dist)? as samples`)

func readsMetrics15s(p planned) bool {
	return len(p.sql) == 1 && reMetrics15s.MatchString(p.sql[0])
}

// TestShortcutRouting: on the 15s lattice with metrics_15s the shortcut
// serves the query; an off-lattice T, R or offset, or a missing metrics_15s,
// sends it to raw SQL. Either way it runs on the grid emitter.
func TestShortcutRouting(t *testing.T) {
	check := func(query, name string, req request, want bool) {
		t.Helper()
		p := runRequest(t, query, req, nil)
		if !isGrid(p.root) || len(p.sql) != 1 {
			t.Fatalf("%s@%s: root %T with %d statements", query, name, p.root, len(p.sql))
		}
		if got := readsMetrics15s(p); got != want {
			t.Errorf("%s@%s: shortcut %v, want %v", query, name, got, want)
		}
		if !want && !strings.Contains(p.sql[0], "samples_v3") {
			t.Errorf("%s@%s: no raw read in %s", query, name, p.sql[0])
		}
	}
	for _, c := range shortcutCases {
		for name, req := range gridRequests {
			check(c.query, name, req, onLattice(req))
			req.noMetrics15s = true
			check(c.query, name+"/nom15s", req, false)
		}
		for name, req := range offLatticeRequests {
			if onLattice(req) {
				t.Fatalf("%s is on the lattice", name)
			}
			check(c.query, name, req, false)
		}
	}
	for _, q := range []string{
		`count_over_time({job="g"} [100s])`,
		`rate({job="g"} [5m] offset 7s)`,
		`sum by (l) (count_over_time({job="g"} [5m] offset 61s))`,
	} {
		for name, req := range gridRequests {
			check(q, name, req, false)
		}
	}
	for _, q := range []string{
		`count_over_time({job="g"} [5m] offset 90s)`,
		`count_over_time({job="g"} | pod="1" [5m])`,
		`rate({job="g"} | line_format "{{__line__}}" [5m])`,
	} {
		check(q, "aligned/300s", gridRequests["aligned/300s"], true)
		check(q, "instant+1234s", offLatticeRequests["instant+1234s"], false)
	}
}

// TestShortcutRawRouteReadsTheWindow: off the lattice, the shortcut's query
// reads exactly (first - R - offset, last - offset] of the raw rows.
func TestShortcutRawRouteReadsTheWindow(t *testing.T) {
	for _, c := range shortcutCases {
		for name, req := range offLatticeRequests {
			p := runRequest(t, c.query, req, nil)
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
		}
	}
}

// TestShortcutLabelFilterOnBothRoutes: a stream label filter applies on the
// shortcut as on raw SQL.
func TestShortcutLabelFilterOnBothRoutes(t *testing.T) {
	q := `count_over_time({job="g"} | pod="1" [5m])`
	for _, req := range []request{gridRequests["aligned/300s"], offLatticeRequests["instant+7s"]} {
		p := runRequest(t, q, req, nil)
		if !strings.Contains(p.sql[0], "(JSONExtractString(labels, 'pod')) == ('1')") {
			t.Errorf("%s: no pod filter in %s", q, p.sql[0])
		}
	}
}

// TestShortcutWindowShapes pins the cell and edge reads, the w = gcd(step, R)
// buckets and the cumulative difference, or one sum for an instant query.
func TestShortcutWindowShapes(t *testing.T) {
	const (
		cumulative = "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
		cellNs     = int64(15 * time.Second)
	)
	r, o, step := 5*time.Minute, 7*time.Minute, 7*time.Minute
	rng := rangeReq(gridD0.Add(2*time.Hour), gridD0.Add(6*time.Hour), step)
	times := refGrid(rng)
	first, last := times[0], times[len(times)-1]
	rn, on, sn := r.Nanoseconds(), o.Nanoseconds(), step.Nanoseconds()
	at := gridD0.Add(4 * time.Hour).UnixNano()
	q := `count_over_time({job="g"} [5m] offset 7m)`
	for _, c := range []struct {
		req      request
		has, not []string
	}{
		{rng, []string{
			fmt.Sprintf("(samples.timestamp_ns) >= (%d)", first-rn-on),
			fmt.Sprintf("(samples.timestamp_ns) < (%d)", last-on),
			fmt.Sprintf("samples.timestamp_ns IN (SELECT arrayJoin(arrayConcat(range(%d, %d, %d), range(%d, %d, %[3]d))))",
				first-on, last-on+1, sn, first-on-rn, last-on-rn+1),
			fmt.Sprintf("[(ts - %d, k), (ts, -k)]", cellNs),
			fmt.Sprintf("intDiv(c + %d, %d) * %[2]d + %[2]d", on, int64(time.Minute)),
			cumulative,
		}, []string{"State("}},
		{instantReq(time.Unix(0, at)), []string{
			fmt.Sprintf("(samples.timestamp_ns) >= (%d)", at-rn-on),
			fmt.Sprintf("(samples.timestamp_ns) < (%d)", at-on),
			fmt.Sprintf("samples.timestamp_ns IN (%d, %d)", at-on, at-on-rn),
			fmt.Sprintf("[(ts - %d, k), (ts, -k)]", cellNs),
		}, []string{" OVER ", "arrayJoin(range(", "State("}},
	} {
		p := runRequest(t, q, c.req, nil)
		if !readsMetrics15s(p) {
			t.Fatalf("%s: not on the shortcut: %v", q, p.sql)
		}
		for _, h := range c.has {
			if !strings.Contains(p.sql[0], h) {
				t.Errorf("%s: no %q in %s", q, h, p.sql[0])
			}
		}
		for _, n := range c.not {
			if strings.Contains(p.sql[0], n) {
				t.Errorf("%s: %q in %s", q, n, p.sql[0])
			}
		}
	}
}

// TestShortcutBinaryRouting: any binary of SQL operands is one SQL query on
// the grid, whatever route each operand takes.
func TestShortcutBinaryRouting(t *testing.T) {
	for _, q := range []string{
		`count_over_time({job="g"} [5m]) / count_over_time({job="g"} [5m] offset 7s)`,
		rawCount5m + ` / count_over_time({job="g"} [5m])`,
		`count_over_time({job="g"} [5m]) - (` + rawCount5m + ` * 2)`,
		`sum(rate({job="g"} [5m])) * 2`,
	} {
		for name, req := range gridRequests {
			p := runRequest(t, q, req, nil)
			if !isGrid(p.root) || len(p.sql) != 1 {
				t.Errorf("%s@%s: root %T with %d statements, want *GridPlanner with 1", q, name, p.root, len(p.sql))
			}
		}
	}
}
