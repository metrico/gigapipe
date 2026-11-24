package promql_transpiler

import (
	"time"

	"github.com/metrico/qryn/v5/reader/promql/promql_transpiler/planner"
	"github.com/prometheus/prometheus/promql/parser"
)

// EvalGrid is the evaluation request of a query: StartMs + k*StepMs up to
// EndMs, or a single instant at StartMs when StepMs == 0.
type EvalGrid struct {
	StartMs int64
	EndMs   int64
	StepMs  int64
	// SubqueryStepMs is the engine's step for a subquery without one, given
	// its range. When nil, the selectors inside such a subquery stay untagged.
	SubqueryStepMs func(rangeMs int64) int64
}

// DefaultSubqueryIntervalMs is the step of a subquery that omits one (e.g.
// `up[1h:]`). It matches Prometheus' default evaluation interval.
func DefaultSubqueryIntervalMs(int64) int64 {
	return time.Minute.Milliseconds()
}

// frame is the set of timestamps a node is evaluated at: PointMs + k*StepMs,
// or the single PointMs when StepMs == 0. A subquery evaluates its inner
// expression at epoch multiples of its step.
type frame struct {
	PointMs int64
	StepMs  int64
}

// TagGrid adds to every vector selector of expr the matcher carrying the grid
// that selector is evaluated on.
func TagGrid(expr parser.Expr, eval EvalGrid) {
	eval.tag(expr, frame{PointMs: eval.StartMs, StepMs: eval.StepMs}, 0)
}

// tag tags the selectors under node, evaluated on f. rangeMs is the range of
// the matrix selector that node belongs to, or 0.
func (eval EvalGrid) tag(node parser.Node, f frame, rangeMs int64) {
	switch n := node.(type) {
	case *parser.VectorSelector:
		if at, ok := eval.at(n.Timestamp, n.StartOrEnd); ok {
			f = frame{PointMs: at}
		}
		n.LabelMatchers = append(n.LabelMatchers, frameGrid(n, f, rangeMs).Matcher())
		return
	case *parser.MatrixSelector:
		eval.tag(n.VectorSelector, f, n.Range.Milliseconds())
		return
	case *parser.SubqueryExpr:
		step := n.Step.Milliseconds()
		if step == 0 && eval.SubqueryStepMs != nil {
			step = eval.SubqueryStepMs(n.Range.Milliseconds())
		}
		if step > 0 {
			eval.tag(n.Expr, frame{StepMs: step}, 0)
		}
		return
	}
	for _, child := range parser.Children(node) {
		eval.tag(child, f, 0)
	}
}

// at resolves an @ modifier to its instant.
func (eval EvalGrid) at(ts *int64, startOrEnd parser.ItemType) (int64, bool) {
	switch {
	case ts != nil:
		return *ts, true
	case startOrEnd == parser.START:
		return eval.StartMs, true
	case startOrEnd == parser.END:
		return eval.EndMs, true
	}
	return 0, false
}

// frameGrid is the grid of vs evaluated on f, shifted by its offset. A
// single-point frame uses the selector's own window as the step.
func frameGrid(vs *parser.VectorSelector, f frame, rangeMs int64) planner.Grid {
	point := f.PointMs - vs.OriginalOffset.Milliseconds()
	if f.StepMs != 0 {
		return planner.NewGrid(point, f.StepMs)
	}
	return planner.NewGrid(point, planner.SelectorWindowMs(rangeMs))
}
