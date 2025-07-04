package traceql_transpiler

import (
	traceql_parser "github.com/metrico/qryn/reader/traceql/parser"
)

type expression struct {
	op       string
	operands []any
}

func optimizeScriptSelectors(script *traceql_parser.TraceQLScript) {
	traceql_parser.Visit(script, func(node any) error {
		if _node, ok := node.(*traceql_parser.Selector); ok && _node.AttrSelector != nil {
			_node.AttrSelector = optimizeSelector(_node.AttrSelector)
		}
		return nil
	})
}

func optimizeSelector(selector *traceql_parser.AttrSelectorExp) *traceql_parser.AttrSelectorExp {
	var (
		_current *expression
		_root    *expression
	)
	tree := selector2Tree(selector, &_root, &_current)
	optimizeTree(tree, nil)
	res := tree2SelectorExp(tree)
	return res
}

func optimizeTree(expr *expression, ptr *any) {
	if expr == nil {
		return
	}
	for i := len(expr.operands) - 1; i >= 0; i-- {
		child := &expr.operands[i]
		if _child, ok := (*child).(*expression); ok {
			optimizeTree(_child, child)
		}
	}
	for i := len(expr.operands) - 1; i >= 0; i-- {
		child := expr.operands[i]
		if _child, ok := child.(*traceql_parser.AttrSelector); ok {
			if _child.True != "true" {
				continue
			}
			if expr.op == "&&" {
				expr.operands = append(expr.operands[:i], expr.operands[i+1:]...)
			} else {
				if ptr != nil && *ptr != nil {
					*ptr = &traceql_parser.AttrSelector{True: "true"}
				} else {
					*expr = expression{}
				}
				return
			}
		}
	}
	if len(expr.operands) == 1 && ptr != nil {
		*ptr = expr.operands[0]
	}
}

func tree2SelectorExp(expr *expression) *traceql_parser.AttrSelectorExp {
	var res traceql_parser.AttrSelectorExp
	var op2Selector = func(op any, res *traceql_parser.AttrSelectorExp) {
		if _op, ok := op.(*traceql_parser.AttrSelector); ok {
			res.Head = _op
			return
		}
		res.ComplexHead = tree2SelectorExp(op.(*expression))
	}
	if len(expr.operands) == 0 {
		return &traceql_parser.AttrSelectorExp{
			Head:        nil,
			ComplexHead: nil,
			AndOr:       "",
			Tail:        nil,
		}

	}
	if len(expr.operands) == 1 {
		op2Selector(expr.operands[0], &res)
		return &res
	}
	res.AndOr = expr.op
	current := &res
	for i := 0; i < len(expr.operands); i += 1 {
		op2Selector(expr.operands[i], current)
		if i+1 < len(expr.operands) {
			current.AndOr = expr.op
			current.Tail = &traceql_parser.AttrSelectorExp{}
			current = current.Tail
		}
	}
	return &res
}

func selector2Tree(selector *traceql_parser.AttrSelectorExp, root **expression, current **expression) *expression {
	if selector == nil {
		return nil
	}

	var newOp any

	if selector.Head != nil {
		newOp = selector.Head
	} else if selector.ComplexHead != nil {
		newOp = selector2Tree(selector.ComplexHead, nil, nil)
	}
	if selector.Tail == nil {
		if *current == nil {
			*root = &expression{operands: []any{newOp}}
			return *root
		} else {
			(*current).operands = append((*current).operands, newOp)
			return *root
		}
	}

	if *current == nil {
		*root = &expression{
			op:       selector.AndOr,
			operands: []any{newOp},
		}
		current = root
	} else if selector.AndOr == "&&" {
		newExpr := &expression{
			op:       "&&",
			operands: []any{newOp},
		}
		(*current).operands = append((*current).operands, newExpr)
		*current = newExpr
	} else if selector.AndOr == "||" {
		(*current).operands = append((*current).operands, newOp)
		*root = &expression{
			op:       "||",
			operands: []any{*root},
		}
		*current = *root
	}

	selector2Tree(selector.Tail, root, current)

	return *root
}
