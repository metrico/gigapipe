package promql_transpiler

import (
	"time"

	"github.com/prometheus/prometheus/promql/parser"
)

// engineLookbackDelta mirrors the engine's default LookbackDelta: an instant
// selector reads up to 5m behind its evaluation time.
const engineLookbackDelta = 5 * time.Minute

// MaxLookback returns how far behind the query start time the expression can
// read: the deepest chain of ranges and offsets among its selectors and
// subqueries, plus the engine's instant-selector lookback delta.
func MaxLookback(expr parser.Expr) time.Duration {
	return nodeLookback(expr) + engineLookbackDelta
}

// nodeLookback is compositional: a subquery's window reaches its own range and
// offset beyond whatever its inner expression already reaches.
func nodeLookback(node parser.Node) time.Duration {
	max := time.Duration(0)
	for _, child := range parser.Children(node) {
		if d := nodeLookback(child); d > max {
			max = d
		}
	}
	switch n := node.(type) {
	case *parser.SubqueryExpr:
		max += n.Range + n.OriginalOffset
	case *parser.MatrixSelector:
		// The inner vector selector's offset is accounted for as a child.
		max += n.Range
	case *parser.VectorSelector:
		max += n.OriginalOffset
	}
	return max
}
