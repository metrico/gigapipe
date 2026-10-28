package logql_transpiler

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// absentLines holds three streams with gaps of their own; their union still
// leaves windows empty. Every stream also has a line on a 5m boundary.
func absentLines() []logLine {
	streams := []map[string]string{
		{"job": "g", "l": "a", "pod": "1"},
		{"job": "g", "l": "a", "pod": "2"},
		{"job": "g", "l": "b", "pod": "1"},
	}
	period := []int{23, 31, 13}
	var lines []logLine
	for m := 0; m < 8*60; m++ {
		for si, s := range streams {
			if m%period[si] != si*5 {
				continue
			}
			ts := gridD0.Add(time.Duration(m)*time.Minute + time.Duration(si*17)*time.Second)
			if m%5 == 0 {
				ts = gridD0.Add(time.Duration(m) * time.Minute)
			}
			lines = append(lines, logLine{fp: uint64(si + 1), labels: s, ts: ts.UnixNano(), line: fmt.Sprintf("m=%d", m)})
		}
	}
	return lines
}

// absentReference gives 1 labelled labels at every T whose window
// (T-offset-R, T-offset] holds none of lines.
func absentReference(lines []logLine, r, offset time.Duration, labels map[string]string,
	times []int64) map[string]map[int64]float64 {
	pts := map[int64]float64{}
	for _, T := range times {
		hi := T - offset.Nanoseconds()
		lo := hi - r.Nanoseconds()
		empty := true
		for _, l := range lines {
			if l.ts > lo && l.ts <= hi {
				empty = false
				break
			}
		}
		if empty {
			pts[T] = 1
		}
	}
	if len(pts) == 0 {
		return map[string]map[int64]float64{}
	}
	return map[string]map[int64]float64{labelsKey(labels): pts}
}

var absentCases = []struct {
	query     string
	r, offset time.Duration
	labels    map[string]string
	none      bool
}{
	{`absent_over_time({job="g", l=~"a|b"} [5m])`, 5 * time.Minute, 0, map[string]string{"job": "g"}, false},
	{`absent_over_time({job="g"} [15m])`, 15 * time.Minute, 0, map[string]string{"job": "g"}, false},
	{`absent_over_time({job="g", pod!="9", pod="1", l="a"} [5m])`, 5 * time.Minute, 0, map[string]string{"job": "g", "l": "a"}, false},
	{`absent_over_time({job="g"} != "x" [5m] offset 7m)`, 5 * time.Minute, 7 * time.Minute, map[string]string{"job": "g"}, false},
	{`absent_over_time({job="g"} | line_format "{{__line__}}" [5m])`, 5 * time.Minute, 0, map[string]string{"job": "g"}, false},
	{`absent_over_time({job="g"} | logfmt [1h] offset 1h)`, time.Hour, time.Hour, map[string]string{"job": "g"}, false},
	{`sum(absent_over_time({job="g"} [5m]))`, 5 * time.Minute, 0, map[string]string{}, false},
	{`absent_over_time({job="none", l=~"x|y"} [5m])`, 5 * time.Minute, 0, map[string]string{"job": "none"}, true},
	{`absent_over_time({job="none"} | line_format "{{__line__}}" [1h] offset 7m)`, time.Hour, 7 * time.Minute, map[string]string{"job": "none"}, true},
}

// TestAbsentOverTimeIsVectorLevel checks that absence is decided over every
// matched stream, on the grid, labelled by the selector's equality matchers.
func TestAbsentOverTimeIsVectorLevel(t *testing.T) {
	all := absentLines()
	for _, c := range absentCases {
		lines := all
		if c.none {
			lines = nil
		}
		for name, req := range gridRequests {
			t.Run(c.query+"@"+name, func(t *testing.T) {
				p := runRequest(t, c.query, req, lines)
				if _, ok := p.root.(*GridPlanner); !ok {
					t.Errorf("root %T, want *GridPlanner", p.root)
				}
				want := absentReference(lines, c.r, c.offset, c.labels, refGrid(req))
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

// TestAbsentOverTimeReadsTheWindowRange checks that absent reads
// (first - R - offset, last - offset] of its grid.
func TestAbsentOverTimeReadsTheWindowRange(t *testing.T) {
	for _, c := range absentCases {
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

// TestAbsentOverTimeBoundary: a line at T counts at T and not at T+R.
func TestAbsentOverTimeBoundary(t *testing.T) {
	at := gridD0.Add(4 * time.Hour)
	lines := []logLine{{fp: 1, labels: map[string]string{"job": "g"}, ts: at.UnixNano(), line: "x"}}
	q := `absent_over_time({job="g"} [5m])`
	for _, c := range []struct {
		t      time.Time
		absent bool
	}{
		{at.Add(-time.Second), true},
		{at, false},
		{at.Add(5*time.Minute - time.Second), false},
		{at.Add(5 * time.Minute), true},
	} {
		p := runRequest(t, q, instantReq(c.t), lines)
		if got := len(p.out) == 1 && p.out[0].Value == 1; got != c.absent {
			t.Errorf("at %s: absent %v, want %v (%+v)", c.t.Sub(at), got, c.absent, p.out)
		}
	}
}
