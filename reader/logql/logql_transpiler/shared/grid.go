package shared

import "fmt"

// EvalGrid holds the evaluation times of a LogQL metric query:
// FirstNs + k*StepNs for k in [0, Points). StepNs is 0 for an instant query.
type EvalGrid struct {
	FirstNs int64
	StepNs  int64
	Points  int64
}

// NewEvalGrid returns the grid of a request: To for an instant query, else
// epoch k*Step from From floored to To ceiled.
func NewEvalGrid(ctx *PlannerContext) (EvalGrid, error) {
	if ctx.Instant {
		return EvalGrid{FirstNs: ctx.To.UnixNano(), Points: 1}, nil
	}
	step := ctx.Step.Nanoseconds()
	if step <= 0 {
		return EvalGrid{}, fmt.Errorf("step must be positive, got %s", ctx.Step)
	}
	first := FloorDiv(ctx.From.UnixNano(), step) * step
	last := -FloorDiv(-ctx.To.UnixNano(), step) * step
	return EvalGrid{FirstNs: first, StepNs: step, Points: (last-first)/step + 1}, nil
}

// At returns the k-th evaluation time.
func (g EvalGrid) At(k int64) int64 {
	return g.FirstNs + k*g.StepNs
}

// LastNs returns the last evaluation time.
func (g EvalGrid) LastNs() int64 {
	return g.At(g.Points - 1)
}

// Index returns k such that At(k) == ts.
func (g EvalGrid) Index(ts int64) (int64, bool) {
	if g.StepNs == 0 {
		return 0, ts == g.FirstNs
	}
	d := ts - g.FirstNs
	if d < 0 || d%g.StepNs != 0 || d/g.StepNs >= g.Points {
		return 0, false
	}
	return d / g.StepNs, true
}

// Slot returns the k whose step [At(k), At(k+1)) holds ts.
func (g EvalGrid) Slot(ts int64) (int64, bool) {
	if g.StepNs == 0 {
		return g.Index(ts)
	}
	k := (ts - g.FirstNs) / g.StepNs
	return k, k >= 0 && k < g.Points
}

// FloorDiv divides rounding toward negative infinity; b must be positive.
func FloorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && a < 0 {
		q--
	}
	return q
}

// Gcd returns the greatest common divisor of a and b.
func Gcd(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}
