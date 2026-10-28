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
	// The streams carry job, l and pod, so without (pod) groups as by (job, l).
	{`sum without (pod) (count_over_time({job="g"} [5m]))`, refQuery{fn: "count_over_time", r: 5 * time.Minute, by: []string{"job", "l"}, agg: "sum"}},
	{`count_over_time({job="g"} [5m] offset 7m)`, refQuery{fn: "count_over_time", r: 5 * time.Minute, offset: 7 * time.Minute}},
	{`sum by (l) (count_over_time({job="g"} [15m] offset 1h))`, refQuery{fn: "count_over_time", r: 15 * time.Minute, offset: time.Hour, by: []string{"l"}, agg: "sum"}},
	{`max by (l) (count_over_time({job="g"} [5m]))`, refQuery{fn: "count_over_time", r: 5 * time.Minute, by: []string{"l"}, agg: "max"}},
	{`max by (l) (count_over_time({job="g"} [5m] offset 1h))`, refQuery{fn: "count_over_time", r: 5 * time.Minute, offset: time.Hour, by: []string{"l"}, agg: "max"}},
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
// buckets and the sliding window, or one sum for an instant query.
func TestShortcutWindowShapes(t *testing.T) {
	// Buckets are gcd(7m, 5m) = 1m wide, so a 5m window is 5 of them.
	const (
		window = "ROWS BETWEEN 4 PRECEDING AND CURRENT ROW"
		cellNs = int64(15 * time.Second)
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
			window,
			"WITH FILL",
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

// reWindowBound matches a label lookup bounded by re-reading a window, which
// computes that window a second time.
var reWindowBound = regexp.MustCompile(`IN \(\s*SELECT fingerprint FROM (pre_without_\d+|pre_by_without_\d+|main)\)`)

// reEdgesJoinedOntoCells matches per-stream buckets read from the cells
// without a GROUP BY, with the edge buckets FULL JOINed onto them.
var reEdgesJoinedOntoCells = regexp.MustCompile(`win_bc as \( SELECT fingerprint, intDiv\(c \+ \d+, \d+\) \* \d+ \+ \d+ as b, ` +
	`cnt as n FROM win_m WHERE \(\(c\) >= \(\d+\)\) and \(\(c\) < \(\d+\)\)\),.* FROM win_bc as win_bc FULL JOIN win_be as win_be ON`)

// TestShortcutReadsEachSourceOnce: a shortcut plan reads metrics_15s twice,
// once for the data and once for the fingerprints that bound the label lookup,
// the raw samples once for the lines on the window edges, and never computes
// its window twice; a sum is summed per group before the window.
func TestShortcutReadsEachSourceOnce(t *testing.T) {
	for _, c := range []struct {
		query      string
		groupFirst bool
	}{
		{`sum(count_over_time({job="g"} [90s]))`, true},
		{`sum by (l) (rate({job="g"} [1h]))`, true},
		{`sum without (pod) (count_over_time({job="g"} [5m]))`, true},
		{`sum by (l) (count_over_time({job="g"} [15m] offset 1h))`, true},
		{`topk(2, sum by (l) (rate({job="g"} [5m])))`, true},
		{`max by (l) (count_over_time({job="g"} [5m]))`, false},
		{`count by (l) (count_over_time({job="g"} [5m] offset 7m))`, false},
		{`count_over_time({job="g"} [5m])`, false},
		{`sum by (l) (count_over_time({job="g"} [5m]) > 6)`, false},
	} {
		for _, name := range []string{"aligned/60s", "instant"} {
			p := runRequest(t, c.query, gridRequests[name], nil)
			if !readsMetrics15s(p) {
				t.Fatalf("%s@%s: not served by the shortcut", c.query, name)
			}
			sql := p.sql[0]
			if n := len(reMetrics15s.FindAllString(sql, -1)); n != 2 {
				t.Errorf("%s@%s: %d metrics_15s reads, want 2", c.query, name, n)
			}
			if n := strings.Count(sql, "FROM samples_v3 as samples"); n != 1 {
				t.Errorf("%s@%s: %d raw samples reads, want 1", c.query, name, n)
			}
			if reWindowBound.MatchString(sql) {
				t.Errorf("%s@%s: label lookup re-reads the window", c.query, name)
			}
			if got := strings.Contains(sql, "win_g"); got != c.groupFirst {
				t.Errorf("%s@%s: groups first %v, want %v", c.query, name, got, c.groupFirst)
			}
			// A range query slides one window function over each stream's or
			// group's buckets, the empty ones filled in, rather than copying
			// every bucket to each point whose window holds it.
			if want := name != "instant"; strings.Contains(sql, "WITH FILL") != want {
				t.Errorf("%s@%s: WITH FILL %v, want %v", c.query, name, !want, want)
			}
			if strings.Contains(sql, "array JOIN range(") {
				t.Errorf("%s@%s: fans buckets out to points", c.query, name)
			}
			// WITH FILL fills each stream's buckets only when the server fills by
			// sort prefix; a profile can turn that off, so the plan pins it.
			if want := name != "instant"; strings.Contains(sql, "use_with_fill_by_sorting_prefix=1") != want {
				t.Errorf("%s@%s: fill by sort prefix pinned %v, want %v", c.query, name, !want, want)
			}
			// The cell read already sums each stream's cells per bucket; the
			// per-stream windows join the edges onto those rows as they are.
			if !c.groupFirst && name != "instant" && !reEdgesJoinedOntoCells.MatchString(sql) {
				t.Errorf("%s@%s: regroups the per-stream buckets", c.query, name)
			}
		}
	}
}

// TestShortcutStartCountCutoff: a shortcut operand whose read range reaches
// the start_count cutoff takes the start lines from the cells there, reads
// the raw edges only before the cutoff, and none when no edge lies before
// it; one whose range lies before the cutoff keeps the plan it has without.
func TestShortcutStartCountCutoff(t *testing.T) {
	type operand struct{ r, offset time.Duration }
	const raw = "FROM samples_v3 as samples"
	for _, c := range []struct {
		query    string
		operands []operand
	}{
		{`count_over_time({job="g"} [5m])`, []operand{{5 * time.Minute, 0}}},
		{`rate({job="g"} [45s])`, []operand{{45 * time.Second, 0}}},
		{`sum by (l) (rate({job="g"} [1h]))`, []operand{{time.Hour, 0}}},
		{`max by (l) (count_over_time({job="g"} [5m]))`, []operand{{5 * time.Minute, 0}}},
		{`count_over_time({job="g"} [5m] offset 7m)`, []operand{{5 * time.Minute, 7 * time.Minute}}},
		{`sum by (l) (count_over_time({job="g"} [15m] offset 1h))`, []operand{{15 * time.Minute, time.Hour}}},
		{`count_over_time({job="g"} [5m]) / count_over_time({job="g"} [5m] offset 7s)`, []operand{{5 * time.Minute, 0}}},
		{`sum(rate({job="g"} [5m])) * 2`, []operand{{5 * time.Minute, 0}}},
		{`count_over_time({job="g"} [5m]) - count_over_time({job="g", l="a"} [5m] offset 5m)`,
			[]operand{{5 * time.Minute, 0}, {5 * time.Minute, 5 * time.Minute}}},
	} {
		for _, name := range []string{"aligned/60s", "unaligned/300s", "instant", "instant+7m"} {
			req := gridRequests[name]
			base := runRequest(t, c.query, req, nil).sql[0]
			times := refGrid(req)
			for _, cut := range []time.Duration{-24 * time.Hour, 3 * time.Hour,
				3*time.Hour + 22*time.Minute + 45*time.Second, 4 * time.Hour, 24 * time.Hour} {
				at := gridD0.Add(cut)
				req.startCount = &at
				sql := runRequest(t, c.query, req, nil).sql[0]
				id := fmt.Sprintf("%s@%s/cutoff=%v", c.query, name, cut)
				starting, edgeless := 0, 0
				for _, o := range c.operands {
					lo := times[0] - o.r.Nanoseconds() - o.offset.Nanoseconds()
					hi := times[len(times)-1] - o.offset.Nanoseconds()
					if at.UnixNano() <= hi {
						starting++
					}
					if at.UnixNano() <= lo {
						edgeless++
					}
				}
				if starting == 0 {
					if sql != base {
						t.Errorf("%s: plan differs from the one without a cutoff", id)
					}
					continue
				}
				starts := fmt.Sprintf("sumIf(samples.start_count, samples.timestamp_ns >= %d", at.UnixNano())
				if n := strings.Count(sql, starts); n != 2*starting {
					t.Errorf("%s: %d start_count reads, want %d", id, n, 2*starting)
				}
				if n, want := strings.Count(sql, raw), strings.Count(base, raw)-edgeless; n != want {
					t.Errorf("%s: %d raw reads, want %d", id, n, want)
				}
				bounded := strings.Contains(sql, fmt.Sprintf("(samples.timestamp_ns) <= (%d)", at.UnixNano()-1))
				if want := starting > edgeless; bounded != want {
					t.Errorf("%s: edge read bounded by the cutoff %v, want %v", id, bounded, want)
				}
				if want := !req.instant; strings.Contains(sql, "+ sum(starts) OVER") != want {
					t.Errorf("%s: start lines in the window frame %v, want %v", id, !want, want)
				}
			}
		}
	}
	// A fresh install's cutoff, 0, covers every cell.
	epoch := time.Unix(0, 0)
	req := gridRequests["aligned/60s"]
	req.startCount = &epoch
	if sql := runRequest(t, `count_over_time({job="g"} [5m])`, req, nil).sql[0]; strings.Contains(sql, raw) ||
		!strings.Contains(sql, "sumIf(samples.start_count, samples.timestamp_ns >= 0") {
		t.Errorf("cutoff 0: %s", sql)
	}
}

// TestShortcutStartCountEdges pins the edge set and the cell read around a
// cutoff: the edges before it are read raw, those from it on are the cells'
// start lines, and cell hi is read for its own.
func TestShortcutStartCountEdges(t *testing.T) {
	r, step := 5*time.Minute, time.Minute
	rn, sn := r.Nanoseconds(), step.Nanoseconds()
	rng := gridRequests["aligned/60s"]
	times := refGrid(rng)
	first, last := times[0], times[len(times)-1]
	q := `count_over_time({job="g"} [5m])`
	for _, c := range []struct {
		req request
		cut int64
		has []string
	}{
		{rng, gridD0.Add(4 * time.Hour).UnixNano(), []string{
			fmt.Sprintf("samples.timestamp_ns IN (SELECT arrayJoin(arrayConcat(range(%d, %d, %d), range(%d, %d, %[3]d))))",
				first, gridD0.Add(4*time.Hour).UnixNano(), sn, first-rn, gridD0.Add(4*time.Hour).UnixNano()),
			fmt.Sprintf("(samples.timestamp_ns) <= (%d)", last),
			fmt.Sprintf("WITH FILL FROM %d TO %d STEP %d", first-rn+sn, last+2*sn, sn),
			"sum(starts) OVER (PARTITION BY fingerprint ORDER BY b ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING)",
		}},
		// The cutoff lies between the first T-R and the first T.
		{rng, first - 2*sn, []string{
			fmt.Sprintf("samples.timestamp_ns IN (SELECT arrayJoin(arrayConcat(range(%d, %d, %d), range(%d, %d, %[3]d))))",
				first, first, sn, first-rn, first-2*sn),
		}},
		// The instant's T lies on the cutoff: only T-R is read raw.
		{instantReq(gridD0.Add(4 * time.Hour)), gridD0.Add(4 * time.Hour).UnixNano(), []string{
			fmt.Sprintf("samples.timestamp_ns IN (%d)", gridD0.Add(4*time.Hour).UnixNano()-rn),
			fmt.Sprintf("toInt64(if(c < %d, cnt, 0) + if(c > %d, starts, 0))", gridD0.Add(4*time.Hour).UnixNano(),
				gridD0.Add(4*time.Hour).UnixNano()-rn),
		}},
	} {
		cut := time.Unix(0, c.cut)
		c.req.startCount = &cut
		sql := runRequest(t, q, c.req, nil).sql[0]
		for _, h := range c.has {
			if !strings.Contains(sql, h) {
				t.Errorf("cutoff %d: no %q in %s", c.cut, h, sql)
			}
		}
	}
}
