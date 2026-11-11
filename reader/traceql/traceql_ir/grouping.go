package traceql_ir

import "errors"

// ErrGroupedOperand is the error for a spanset operator over grouped spansets.
var ErrGroupedOperand = errors.New("spanset operators are not supported for multiple spansets per trace. consider using coalesce()")

// CheckGrouping rejects spanset operators over grouped spansets. A pipeline
// is grouped from a by() stage to the next coalesce(); an operator's operands
// are grouped when the stage before is, or when they end grouped themselves.
func CheckGrouping(p *Pipeline) error {
	_, err := p.grouped(false)
	return err
}

// grouped reports whether p ends grouped when its input is grouped as in.
func (p *Pipeline) grouped(in bool) (bool, error) {
	res := in
	for _, element := range p.Elements {
		switch e := element.(type) {
		case *GroupBy:
			res = true
		case *Coalesce:
			res = false
		case SpansetExpr:
			var err error
			if res, err = spansetGrouped(e, res); err != nil {
				return false, err
			}
		}
	}
	return res, nil
}

// spansetGrouped reports whether expr yields grouped spansets from input
// grouped as in.
func spansetGrouped(expr SpansetExpr, in bool) (bool, error) {
	switch e := expr.(type) {
	case *Group:
		return spansetGrouped(e.Expr, in)
	case *Pipeline:
		return e.grouped(in)
	case *SpansetOp:
		if in {
			return false, ErrGroupedOperand
		}
		for _, operand := range []SpansetExpr{e.LHS, e.RHS} {
			grouped, err := spansetGrouped(operand, false)
			if err != nil {
				return false, err
			}
			if grouped {
				return false, ErrGroupedOperand
			}
		}
		return false, nil
	}
	return in, nil
}
