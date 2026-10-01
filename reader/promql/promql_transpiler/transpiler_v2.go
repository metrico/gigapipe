package promql_transpiler

import (
	"slices"
	"time"

	"github.com/metrico/qryn/v5/reader/promql/metricread"
	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
	"github.com/metrico/qryn/v5/reader/promql/promql_transpiler/optimizer"
	"github.com/prometheus/prometheus/promql/parser"
)

// optimizers replace the nodes they apply to with substitute selectors evaluated at the
// query's evaluation timestamps.
var optimizers = []func(metricread.Grid) optimizer.Optimizer{
	func(grid metricread.Grid) optimizer.Optimizer {
		return &optimizer.Pushdown{Grid: grid, Lookback: EngineLookbackDelta}
	},
}

// TranspileExpressionV2 replaces every node an optimizer applies to, outermost first, with a
// substitute selector. grid holds the timestamps the query is evaluated at.
func TranspileExpressionV2(expr *promql_parser.Expr, grid metricread.Grid) (*promql_parser.Expr, error) {
	earliestMs := EarliestReadNS(expr.Expr, time.UnixMilli(grid.StartMs)) / int64(time.Millisecond)
	_expr, err := Walk(expr, expr.Expr, func(node parser.Expr) (parser.Expr, error) {
		for _, opt := range optimizers {
			_opt := opt(grid)
			if _opt.Applicable(node) {
				return _opt.Optimize(expr, node)
			}
		}
		return node, nil
	})
	if err != nil {
		return nil, err
	}
	expr.Expr = _expr
	expr.Read = read(expr, grid, earliestMs)
	return expr, nil
}

// read records the ranges of expr's pushed-down range functions and whether the engine reads
// any selector itself.
func read(expr *promql_parser.Expr, grid metricread.Grid, earliestMs int64) metricread.Read {
	r := metricread.Read{Grid: grid, EarliestMs: earliestMs}
	for _, sub := range expr.Substitutes {
		if sub.Pushdown.Func != "" {
			r.RangesMs = append(r.RangesMs, sub.Pushdown.RangeMs)
		}
	}
	slices.Sort(r.RangesMs)
	parser.Inspect(expr.Expr, func(node parser.Node, _ []parser.Node) error {
		if vs, ok := node.(*parser.VectorSelector); ok && expr.Substitutes[vs.Name] == nil {
			r.EngineReads = true
		}
		return nil
	})
	return r
}

// Walk calls fn on node, then, unless fn replaced it, on its children. It visits only nodes
// evaluated at the query's own timestamps for their values: it does not enter range or
// subquery selectors, nor the arguments of timestamp and absent.
func Walk(expr *promql_parser.Expr, node parser.Expr, fn func(parser.Expr) (parser.Expr, error)) (parser.Expr, error) {
	res, err := fn(node)
	if err != nil || res != node {
		return res, err
	}
	iterate := func(ps ...*parser.Expr) error {
		for _, p := range ps {
			if *p == nil {
				continue
			}
			child, err := Walk(expr, *p, fn)
			if err != nil {
				return err
			}
			*p = child
		}
		return nil
	}

	switch n := node.(type) {
	case *parser.AggregateExpr:
		err = iterate(&n.Expr, &n.Param)
	case *parser.BinaryExpr:
		err = iterate(&n.LHS, &n.RHS)
	case *parser.Call:
		if n.Func.Name == "timestamp" || n.Func.Name == "absent" {
			break
		}
		for i := range n.Args {
			if err = iterate(&n.Args[i]); err != nil {
				break
			}
		}
	case *parser.ParenExpr:
		err = iterate(&n.Expr)
	case *parser.UnaryExpr:
		err = iterate(&n.Expr)
	case *parser.StepInvariantExpr:
		err = iterate(&n.Expr)
	}
	if err != nil {
		return nil, err
	}
	return node, nil
}
