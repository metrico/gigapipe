package transpiler

import (
	"github.com/metrico/qryn/reader/promql/parser"
	"github.com/metrico/qryn/reader/promql/transpiler/optimizer"
	parser2 "github.com/prometheus/prometheus/promql/parser"
)

var optimizers = []func() optimizer.Optimizer{
	func() optimizer.Optimizer { return &optimizer.VectorRange{} },
}

func TranspileExpressionV2(expr *parser.Expr) (*parser.Expr, error) {
	_expr, err := Walk(expr, expr.Expr, func(node parser2.Expr) (parser2.Expr, error) {
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
	return expr, nil
}

func Walk(expr *parser.Expr, node parser2.Expr, fn func(parser2.Expr) (parser2.Expr, error)) (parser2.Expr, error) {
	var err error
	iterate := func(ps []*parser2.Expr) {
		for i := range ps {
			var _child parser2.Expr
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
	case *parser2.AggregateExpr:
		_node := node.(*parser2.AggregateExpr)
		iterate([]*parser2.Expr{&_node.Expr, &_node.Param})
	case *parser2.BinaryExpr:
		_node := node.(*parser2.BinaryExpr)
		iterate([]*parser2.Expr{&_node.LHS, &_node.RHS})
	case *parser2.Call:
		_node := node.(*parser2.Call)
		_exprs := make([]*parser2.Expr, len(_node.Args))
		for i := range _node.Args {
			_exprs[i] = &_node.Args[i]
		}
		iterate(_exprs)
	case *parser2.MatrixSelector:
		_node := node.(*parser2.MatrixSelector)
		iterate([]*parser2.Expr{&_node.VectorSelector})
	case *parser2.SubqueryExpr:
		_node := node.(*parser2.SubqueryExpr)
		iterate([]*parser2.Expr{&_node.Expr})
	case *parser2.NumberLiteral:
		// No-op
	case *parser2.ParenExpr:
		_node := node.(*parser2.ParenExpr)
		iterate([]*parser2.Expr{&_node.Expr})
		// No-op
	case *parser2.StringLiteral:
		// No-op
	case *parser2.UnaryExpr:
		_node := node.(*parser2.UnaryExpr)
		iterate([]*parser2.Expr{&_node.Expr})
	case *parser2.VectorSelector:
		// No-op
	case *parser2.StepInvariantExpr:
		_node := node.(*parser2.StepInvariantExpr)
		iterate([]*parser2.Expr{&_node.Expr})
	}
	if err != nil {
		return nil, err
	}

	return fn(node)
}
