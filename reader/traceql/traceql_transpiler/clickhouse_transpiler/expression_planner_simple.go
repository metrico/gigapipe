package clickhouse_transpiler

import (
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
)

type simpleExpressionPlanner struct {
	selector *traceql_ir.Selector
	prefix   string
	// operand is unset only to estimate a whole-query `{}` as the attribute-less planner.
	operand bool

	//Analyze results
	termIdx []*attrComparison
	cond    *condition

	terms map[string]int
}

// attrComparison is a filter leaf the attribute-condition planner evaluates:
// `Attr Op Value`, or a bare `true`/`false` as Attr.
type attrComparison struct {
	Attr  string
	Op    string
	Value traceql_ir.Static
}

func (c *attrComparison) String() string {
	return c.Attr + " " + c.Op + " " + c.Value.Text
}

// planner plans the selector's span rows: every span in range for `{}`, the
// attribute-condition planner for a boolean combination of `attribute op
// literal`, else a pivot of the attributes the filter reads.
func (p *simpleExpressionPlanner) planner() (shared.SQLRequestPlanner, error) {
	if p.selector.Filter == nil {
		return &AllSpansPlanner{}, nil
	}
	if err := traceql_ir.CheckFilter(p.selector.Filter); err != nil {
		return nil, err
	}
	if p.analyze() {
		return &AttrConditionPlanner{
			Main:  NewInitIndexPlanner(true),
			Terms: p.termIdx,
			Conds: p.cond,
		}, nil
	}
	return &FieldFilterPlanner{Filter: p.selector.Filter, Prefix: p.prefix}, nil
}

// planEval estimates the selector's cost; a whole-query `{}` (operand unset)
// estimates the attribute-less planner.
func (p *simpleExpressionPlanner) planEval() (shared.SQLRequestPlanner, error) {
	if p.selector.Filter == nil {
		if p.operand {
			return &AllSpansEvaluatorPlanner{Prefix: p.prefix}, nil
		}
		return &AttrlessEvaluatorPlanner{Prefix: p.prefix}, nil
	}
	if err := traceql_ir.CheckFilter(p.selector.Filter); err != nil {
		return nil, err
	}
	if p.analyze() {
		return &AttrConditionEvaluatorPlanner{
			Main: &AttrConditionPlanner{
				Main:  NewInitIndexPlanner(true),
				Terms: p.termIdx,
				Conds: p.cond,
			},
			Prefix: p.prefix,
		}, nil
	}
	return &FieldFilterEvaluatorPlanner{Filter: p.selector.Filter, Prefix: p.prefix}, nil
}

// analyze splits the filter into attribute comparisons and the condition over
// them, and reports whether the filter is a boolean combination of them.
func (p *simpleExpressionPlanner) analyze() bool {
	p.terms = make(map[string]int)
	p.termIdx = nil
	p.cond = p.analyzeCond(p.selector.Filter)
	return p.cond != nil
}

func (p *simpleExpressionPlanner) analyzeCond(exp traceql_ir.FieldExpr) *condition {
	if op, ok := exp.(*traceql_ir.BinaryOp); ok && traceql_ir.IsLogical(op.Op) {
		lhs := p.analyzeCond(op.LHS)
		rhs := p.analyzeCond(op.RHS)
		if lhs == nil || rhs == nil {
			return nil
		}
		return &condition{simpleIdx: -1, op: op.Op, complex: []*condition{lhs, rhs}}
	}
	cmp := attrComparisonOf(exp)
	if cmp == nil {
		return nil
	}
	term := cmp.String()
	if p.terms[term] != 0 {
		return &condition{simpleIdx: p.terms[term] - 1}
	}
	p.termIdx = append(p.termIdx, cmp)
	p.terms[term] = len(p.termIdx)
	return &condition{simpleIdx: len(p.termIdx) - 1}
}

// attrComparisonOf returns e as a comparison the attribute-condition planner
// evaluates, or nil.
func attrComparisonOf(e traceql_ir.FieldExpr) *attrComparison {
	if s, ok := e.(*traceql_ir.Static); ok && s.Kind == traceql_ir.ValueBool {
		return &attrComparison{Attr: s.Text}
	}
	op, ok := e.(*traceql_ir.BinaryOp)
	if !ok || !traceql_ir.IsComparison(op.Op) {
		return nil
	}
	attr, ok := op.LHS.(*traceql_ir.Attribute)
	if !ok {
		return nil
	}
	lit, ok := op.RHS.(*traceql_ir.Static)
	if !ok || !attrComparable(attr.Name, op.Op, lit.Kind) {
		return nil
	}
	return &attrComparison{Attr: attr.Name, Op: op.Op, Value: *lit}
}

// attrComparable reports whether the attribute-condition planner evaluates
// `name op literal` with a literal of the given kind.
func attrComparable(name, op string, kind traceql_ir.ValueKind) bool {
	isRegex := traceql_ir.IsRegex(op)
	isEquality := op == "=" || op == "!="
	isString := kind == traceql_ir.ValueString && (isEquality || isRegex) ||
		kind == traceql_ir.ValueIdent && isEquality
	switch {
	case name == "duration" || name == "span:duration":
		return kind == traceql_ir.ValueDuration && !isRegex
	case name == "status" || name == "kind":
		return kind == traceql_ir.ValueIdent && isEquality
	case name == "name" || name == "span:name" || name == "statusMessage" || name == "rootName" || name == "rootServiceName":
		return isString
	}
	if _, ok := traceql_ir.AttributeKey(name); ok {
		return isString || kind == traceql_ir.ValueNumber && !isRegex
	}
	return false
}
