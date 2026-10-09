package planner

import (
	"fmt"

	"github.com/metrico/qryn/v5/reader/model"
	"github.com/prometheus/prometheus/model/labels"
)

// GridLabel names the synthetic matcher that carries a selector's Grid from
// the transpiler to Select. It is a != matcher so absent() never copies it
// into its output labels.
const GridLabel = "__grid__"

// Grid is the set of timestamps a selector is evaluated at: PhaseMs + k*StepMs,
// with PhaseMs in [0, StepMs).
type Grid struct {
	PhaseMs int64
	StepMs  int64
}

// NewGrid normalises point onto the grid of the given step.
func NewGrid(pointMs, stepMs int64) Grid {
	return Grid{PhaseMs: ((pointMs % stepMs) + stepMs) % stepMs, StepMs: stepMs}
}

// LatticeMs is the metrics_15s bucket width.
const LatticeMs = 15000

// OnLattice reports whether metrics_15s can serve a selector on g whose window
// is rangeMs: the phase and some bucket width dividing both the step and the
// range are multiples of LatticeMs.
func (g Grid) OnLattice(rangeMs int64) bool {
	return g.PhaseMs%LatticeMs == 0 && gcd(g.StepMs, rangeMs)%LatticeMs == 0
}

// Pushable reports whether metrics_15s can serve a selector on g with range
// rangeMs (0 for none) in buckets wider than one cell.
func (g Grid) Pushable(rangeMs int64) bool {
	return g.OnLattice(SelectorWindowMs(rangeMs)) && GridEdgeMs(g, rangeMs) > LatticeMs
}

// SelectorWindowMs is the window a selector reads at each evaluation point:
// its range, or the lookback delta for a bare selector (rangeMs == 0).
func SelectorWindowMs(rangeMs int64) int64 {
	if rangeMs == 0 {
		return model.LookbackDeltaMs
	}
	return rangeMs
}

func gcd(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// Matcher returns the grid matcher carrying g.
func (g Grid) Matcher() *labels.Matcher {
	return labels.MustNewMatcher(labels.MatchNotEqual, GridLabel, fmt.Sprintf("%d:%d", g.PhaseMs, g.StepMs))
}

// GridFromMatchers returns the grid carried by matchers and the matchers
// without it.
func GridFromMatchers(matchers []*labels.Matcher) (Grid, []*labels.Matcher, bool) {
	var (
		g     Grid
		found bool
		rest  = make([]*labels.Matcher, 0, len(matchers))
	)
	for _, m := range matchers {
		if m.Name != GridLabel {
			rest = append(rest, m)
			continue
		}
		if _, err := fmt.Sscanf(m.Value, "%d:%d", &g.PhaseMs, &g.StepMs); err == nil && g.StepMs > 0 {
			found = true
		}
	}
	return g, rest, found
}
