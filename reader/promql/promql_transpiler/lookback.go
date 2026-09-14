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

// EarliestReadNS returns the earliest nanosecond timestamp the expression can
// read when its evaluation starts at start: the query start or the smallest
// literal @ timestamp, whichever is earlier, minus the maximum lookback.
func EarliestReadNS(expr parser.Expr, start time.Time) int64 {
	earliest := start
	if ts, ok := MinAtTimestamp(expr); ok {
		if t := time.UnixMilli(ts); t.Before(earliest) {
			earliest = t
		}
	}
	return earliest.Add(-MaxLookback(expr)).UnixNano()
}

// MinAtTimestamp returns the smallest literal @ timestamp (unix ms) among the
// expression's selectors and subqueries, and whether one exists. @ start() and
// @ end() resolve to the query bounds and never precede the query start, so
// they are ignored here.
func MinAtTimestamp(node parser.Node) (int64, bool) {
	min, found := int64(0), false
	consider := func(ts *int64) {
		if ts != nil && (!found || *ts < min) {
			min, found = *ts, true
		}
	}
	switch n := node.(type) {
	case *parser.VectorSelector:
		consider(n.Timestamp)
	case *parser.SubqueryExpr:
		consider(n.Timestamp)
	}
	for _, child := range parser.Children(node) {
		if ts, ok := MinAtTimestamp(child); ok && (!found || ts < min) {
			min, found = ts, true
		}
	}
	return min, found
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
