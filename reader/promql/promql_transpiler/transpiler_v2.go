package promql_transpiler

import (
	"github.com/metrico/qryn/reader/promql/promql_parser"
	"github.com/metrico/qryn/reader/promql/promql_transpiler/optimizer"
	"github.com/metrico/qryn/reader/promql/promql_transpiler/planner"
	"github.com/prometheus/prometheus/promql/parser"
)

var optimizers = []func() optimizer.Optimizer{
	func() optimizer.Optimizer { return &optimizer.VectorRange{} },
}

func TranspileExpressionV2(expr *promql_parser.Expr) (*promql_parser.Expr, error) {
	_expr, err := Walk(expr, expr.Expr, func(node parser.Expr) (parser.Expr, error) {
		for _, opt := range optimizers {
			_opt := opt()
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
	for _, s := range expr.Substitutes {
		if !s.Notes.NeedsLabelsValues {
			continue
		}
		s.Request = &planner.LabelsPlanner{Main: s.Request}
	}
	return expr, nil
}

func Walk(expr *promql_parser.Expr, node parser.Expr, fn func(parser.Expr) (parser.Expr, error)) (parser.Expr, error) {
	var err error
	iterate := func(ps []*parser.Expr) {
		for i := range ps {
			var _child parser.Expr
			_child, err = Walk(expr, *ps[i], fn)
			if err != nil {
				return
			}
			if _child != *ps[i] {
				*ps[i] = _child
			}
		}
	}

	switch node.(type) {
	case *parser.AggregateExpr:
		_node := node.(*parser.AggregateExpr)
		iterate([]*parser.Expr{&_node.Expr, &_node.Param})
	case *parser.BinaryExpr:
		_node := node.(*parser.BinaryExpr)
		iterate([]*parser.Expr{&_node.LHS, &_node.RHS})
	case *parser.Call:
		_node := node.(*parser.Call)
		_exprs := make([]*parser.Expr, len(_node.Args))
		for i := range _node.Args {
			_exprs[i] = &_node.Args[i]
		}
		iterate(_exprs)
	case *parser.MatrixSelector:
		_node := node.(*parser.MatrixSelector)
		iterate([]*parser.Expr{&_node.VectorSelector})
	case *parser.SubqueryExpr:
		_node := node.(*parser.SubqueryExpr)
		iterate([]*parser.Expr{&_node.Expr})
	case *parser.NumberLiteral:
		// No-op
	case *parser.ParenExpr:
		_node := node.(*parser.ParenExpr)
		iterate([]*parser.Expr{&_node.Expr})
		// No-op
	case *parser.StringLiteral:
		// No-op
	case *parser.UnaryExpr:
		_node := node.(*parser.UnaryExpr)
		iterate([]*parser.Expr{&_node.Expr})
	case *parser.VectorSelector:
		// No-op
	case *parser.StepInvariantExpr:
		_node := node.(*parser.StepInvariantExpr)
		iterate([]*parser.Expr{&_node.Expr})
	}
	if err != nil {
		return nil, err
	}

	return fn(node)
}
