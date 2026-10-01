package ruler

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
)

const staleMarkerBits = 0x7ff0000000000002

func vecOf(instances ...string) promql.Vector {
	v := promql.Vector{}
	for _, i := range instances {
		v = append(v, promql.Sample{F: 1, Metric: labels.FromStrings("__name__", "src", "instance", i)})
	}
	return v
}

// markers returns the instance label of each stale marker in v, checking it
// is stamped at ts.
func markers(t *testing.T, v promql.Vector, ts time.Time) []string {
	t.Helper()
	var out []string
	for _, s := range v {
		if math.Float64bits(s.F) != staleMarkerBits {
			continue
		}
		if s.T != ts.UnixMilli() {
			t.Errorf("marker for %s at %d, want %d", s.Metric, s.T, ts.UnixMilli())
		}
		if s.Metric.Get("__name__") != "rec" {
			t.Errorf("marker labels %s, want the recorded series' name", s.Metric)
		}
		out = append(out, s.Metric.Get("instance"))
	}
	return out
}

func newVanishManager(eval *fakeEvaluator, writer *fakeWriter) *RuleManager {
	m := NewRuleManager(eval, &fakeReader{}, writer, time.Minute)
	m.ctx = context.Background()
	return m
}

var (
	vanishRule = Rule{Record: "rec", Expr: "src"}
	t0         = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t1         = t0.Add(15 * time.Second)
	t2         = t1.Add(15 * time.Second)
)

func TestVanish_MissingSeriesGetsOneMarkerAtTheEvaluationTime(t *testing.T) {
	eval := &fakeEvaluator{vec: vecOf("a", "b")}
	writer := &fakeWriter{}
	m := newVanishManager(eval, writer)

	m.evaluateRecordingRule("ns", "g", vanishRule, t0)
	eval.vec = vecOf("a")
	m.evaluateRecordingRule("ns", "g", vanishRule, t1)
	m.evaluateRecordingRule("ns", "g", vanishRule, t2)

	if got := markers(t, writer.writes[0], t0); len(got) != 0 {
		t.Errorf("first evaluation markers = %v, want none", got)
	}
	if got := markers(t, writer.writes[1], t1); len(got) != 1 || got[0] != "b" {
		t.Errorf("second evaluation markers = %v, want [b]", got)
	}
	if got := markers(t, writer.writes[2], t2); len(got) != 0 {
		t.Errorf("third evaluation markers = %v, want none", got)
	}
}

func TestVanish_EmptyResultMarksEverySeries(t *testing.T) {
	eval := &fakeEvaluator{vec: vecOf("a", "b")}
	writer := &fakeWriter{}
	m := newVanishManager(eval, writer)

	m.evaluateRecordingRule("ns", "g", vanishRule, t0)
	eval.vec = promql.Vector{}
	m.evaluateRecordingRule("ns", "g", vanishRule, t1)

	if got := markers(t, writer.writes[1], t1); len(got) != 2 {
		t.Errorf("markers = %v, want a and b", got)
	}
	if n := len(writer.writes[1]); n != 2 {
		t.Errorf("written %d samples, want only the 2 markers", n)
	}
}

func TestVanish_NoMarkerWhileTheSeriesIsStillThere(t *testing.T) {
	eval := &fakeEvaluator{vec: vecOf("a")}
	writer := &fakeWriter{}
	m := newVanishManager(eval, writer)

	m.evaluateRecordingRule("ns", "g", vanishRule, t0)
	m.evaluateRecordingRule("ns", "g", vanishRule, t1)

	if got := markers(t, writer.writes[1], t1); len(got) != 0 {
		t.Errorf("markers = %v, want none", got)
	}
}

func TestVanish_NoMarkersAfterARestart(t *testing.T) {
	eval := &fakeEvaluator{vec: vecOf("a", "b")}
	newVanishManager(eval, &fakeWriter{}).evaluateRecordingRule("ns", "g", vanishRule, t0)

	eval.vec = vecOf("a")
	writer := &fakeWriter{}
	newVanishManager(eval, writer).evaluateRecordingRule("ns", "g", vanishRule, t1)

	if got := markers(t, writer.writes[0], t1); len(got) != 0 {
		t.Errorf("markers after restart = %v, want none", got)
	}
}

func TestVanish_NoMarkersForANewRule(t *testing.T) {
	eval := &fakeEvaluator{vec: vecOf("a", "b")}
	writer := &fakeWriter{}
	m := newVanishManager(eval, writer)

	m.evaluateRecordingRule("ns", "g", vanishRule, t0)
	eval.vec = vecOf("a")
	m.evaluateRecordingRule("ns", "g", Rule{Record: "rec", Expr: "src", Labels: map[string]string{"k": "v"}}, t1)

	if got := markers(t, writer.writes[1], t1); len(got) != 0 {
		t.Errorf("markers for a new rule = %v, want none", got)
	}
}

func TestVanish_FailedEvaluationKeepsTheLastResult(t *testing.T) {
	eval := &fakeEvaluator{vec: vecOf("a", "b")}
	writer := &fakeWriter{}
	m := newVanishManager(eval, writer)

	m.evaluateRecordingRule("ns", "g", vanishRule, t0)
	eval.err = context.DeadlineExceeded
	m.evaluateRecordingRule("ns", "g", vanishRule, t1)
	eval.err, eval.vec = nil, vecOf("a")
	m.evaluateRecordingRule("ns", "g", vanishRule, t2)

	if got := markers(t, writer.writes[len(writer.writes)-1], t2); len(got) != 1 || got[0] != "b" {
		t.Errorf("markers = %v, want [b]", got)
	}
}
