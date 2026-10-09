package logql_transpiler

import (
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"
)

// TestShortcutEvaluatesOnTheGridOnClickHouse runs every shortcut query on
// ClickHouse, on and off the 15s lattice and without metrics_15s, and checks
// it against (T-offset-R, T] and the route it took.
func TestShortcutEvaluatesOnTheGridOnClickHouse(t *testing.T) {
	base := chLogURL(t)
	lines := gridLines()
	reqs := maps.Clone(gridRequests)
	maps.Copy(reqs, offLatticeRequests)
	for name, req := range gridRequests {
		req.noMetrics15s = true
		reqs[name+"/nom15s"] = req
	}
	for _, cluster := range chModes(t, base) {
		for _, c := range shortcutCases {
			for name, req := range reqs {
				req.cluster = cluster
				t.Run(fmt.Sprintf("%s@%s/cluster=%v", c.query, name, cluster), func(t *testing.T) {
					db := newChDB(base)
					defer db.Close()
					p := serveRequest(t, c.query, req, db)
					if want := onLattice(req) && !req.noMetrics15s; readsMetrics15s(p) != want {
						t.Errorf("shortcut %v, want %v", !want, want)
					}
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
}

// TestShortcutBinariesOnClickHouse checks binaries with a shortcut operand
// point by point: series and points on one side only are dropped, and the
// operands may take different routes.
func TestShortcutBinariesOnClickHouse(t *testing.T) {
	base := chLogURL(t)
	lines := gridLines()
	var linesA []logLine
	for _, l := range lines {
		if l.labels["l"] == "a" {
			linesA = append(linesA, l)
		}
	}
	count := refQuery{fn: "count_over_time", r: 5 * time.Minute}
	shifted := refQuery{fn: "count_over_time", r: 5 * time.Minute, offset: 5 * time.Minute}
	off7s := refQuery{fn: "count_over_time", r: 5 * time.Minute, offset: 7 * time.Second}
	for _, c := range []struct {
		query       string
		left, right refQuery
		rightLines  []logLine
	}{
		{`count_over_time({job="g"} [5m]) - count_over_time({job="g", l="a"} [5m] offset 5m)`, count, shifted, linesA},
		{`count_over_time({job="g"} [5m]) - count_over_time(` + rawSel + ` [5m] offset 5m)`, count, shifted, lines},
		{`count_over_time({job="g"} [5m] offset 7s) - count_over_time({job="g"} [5m])`, off7s, count, lines},
	} {
		for _, cluster := range chModes(t, base) {
			for name, req := range gridRequests {
				req.cluster = cluster
				t.Run(fmt.Sprintf("%s@%s/cluster=%v", c.query, name, cluster), func(t *testing.T) {
					db := newChDB(base)
					defer db.Close()
					p := serveRequest(t, c.query, req, db)
					times := refGrid(req)
					want := joinSeries(reference(lines, c.left, times), reference(c.rightLines, c.right, times),
						func(x, y float64) float64 { return x - y })
					if d := diffSeries(gotSeries(p), want); len(d) > 0 {
						t.Errorf("%v", d[:min(len(d), 6)])
					}
				})
			}
		}
	}
}
