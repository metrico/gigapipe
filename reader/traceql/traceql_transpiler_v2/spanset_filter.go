package traceql_transpiler_v2

import (
	"fmt"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v4/reader/traceql/tempo"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

// planSpansetFilter converts a SpansetFilter to SQL condition planner.
func (p *TempoPlanner) planSpansetFilter(filter *tempo.SpansetFilter) (SQLConditionPlanner, error) {
	if filter == nil || filter.Expression == nil {
		return &TrueConditionPlanner{}, nil
	}
	return p.planFieldExpression(filter.Expression)
}

// planFieldExpression converts a FieldExpression to SQL condition planner.
func (p *TempoPlanner) planFieldExpression(expr tempo.FieldExpression) (SQLConditionPlanner, error) {
	switch e := expr.(type) {
	case *tempo.BinaryOperation:
		return p.planBinaryOperation(e)
	case tempo.UnaryOperation:
		return p.planUnaryOperation(e)
	case tempo.Attribute:
		return p.planAttributeAsCondition(e)
	case tempo.Static:
		return p.planStaticAsCondition(e)
	default:
		return nil, fmt.Errorf("unsupported field expression type: %T", expr)
	}
}

// planBinaryOperation handles binary operations like (attr = value), (a && b), etc.
func (p *TempoPlanner) planBinaryOperation(op *tempo.BinaryOperation) (SQLConditionPlanner, error) {
	switch op.Op {
	case tempo.OpAnd:
		left, err := p.planFieldExpression(op.LHS)
		if err != nil {
			return nil, err
		}
		right, err := p.planFieldExpression(op.RHS)
		if err != nil {
			return nil, err
		}
		return &BinaryConditionPlanner{Left: left, Right: right, Op: "AND"}, nil

	case tempo.OpOr:
		left, err := p.planFieldExpression(op.LHS)
		if err != nil {
			return nil, err
		}
		right, err := p.planFieldExpression(op.RHS)
		if err != nil {
			return nil, err
		}
		return &BinaryConditionPlanner{Left: left, Right: right, Op: "OR"}, nil

	case tempo.OpEqual, tempo.OpNotEqual, tempo.OpGreater, tempo.OpGreaterEqual,
		tempo.OpLess, tempo.OpLessEqual, tempo.OpRegex, tempo.OpNotRegex:
		return p.planComparisonOperation(op)

	default:
		return nil, fmt.Errorf("unsupported binary operator: %v", op.Op)
	}
}

// planComparisonOperation handles comparison operations (=, !=, >, <, >=, <=, =~, !~).
func (p *TempoPlanner) planComparisonOperation(op *tempo.BinaryOperation) (SQLConditionPlanner, error) {
	// Check if LHS is an attribute
	attr, isAttr := op.LHS.(tempo.Attribute)
	if !isAttr {
		// Maybe RHS is the attribute (e.g., "value" = attr)
		if rAttr, ok := op.RHS.(tempo.Attribute); ok {
			attr = rAttr
			// Swap LHS and RHS for comparison
			return p.planAttributeComparison(attr, op.Op, op.LHS)
		}
		return nil, fmt.Errorf("expected attribute in comparison, got %T and %T", op.LHS, op.RHS)
	}

	return p.planAttributeComparison(attr, op.Op, op.RHS)
}

// planAttributeComparison handles comparisons like (attr op value).
func (p *TempoPlanner) planAttributeComparison(attr tempo.Attribute, op tempo.Operator, value tempo.FieldExpression) (SQLConditionPlanner, error) {
	// Handle intrinsics specially
	if attr.Intrinsic != tempo.IntrinsicNone {
		return p.planIntrinsicComparison(attr, op, value)
	}

	// Handle scoped attributes
	return p.planScopedAttributeComparison(attr, op, value)
}

// planUnaryOperation handles unary operations like (!expr).
func (p *TempoPlanner) planUnaryOperation(op tempo.UnaryOperation) (SQLConditionPlanner, error) {
	switch op.Op {
	case tempo.OpNot:
		inner, err := p.planFieldExpression(op.Expression)
		if err != nil {
			return nil, err
		}
		return &NotConditionPlanner{Inner: inner}, nil
	default:
		return nil, fmt.Errorf("unsupported unary operator: %v", op.Op)
	}
}

// planAttributeAsCondition handles bare attributes (existence check).
func (p *TempoPlanner) planAttributeAsCondition(attr tempo.Attribute) (SQLConditionPlanner, error) {
	// A bare attribute means existence check
	if attr.Intrinsic != tempo.IntrinsicNone {
		// Intrinsics always exist
		return &TrueConditionPlanner{}, nil
	}

	key := p.getAttributeKey(attr)
	return &AttributeExistsPlanner{Key: key}, nil
}

// planStaticAsCondition handles bare static values like {true} or {false}.
func (p *TempoPlanner) planStaticAsCondition(static tempo.Static) (SQLConditionPlanner, error) {
	if static.Type == tempo.TypeBoolean {
		if b, ok := static.Bool(); ok {
			if b {
				return &TrueConditionPlanner{}, nil
			}
			return &FalseConditionPlanner{}, nil
		}
	}
	return &TrueConditionPlanner{}, nil
}

// planScalarFilter handles scalar filters like | count() > 5.
func (p *TempoPlanner) planScalarFilter(filter tempo.ScalarFilter) (SQLConditionPlanner, error) {
	// For now, return true - scalar filters require post-aggregation
	// TODO: Implement proper scalar filter handling
	return &TrueConditionPlanner{}, nil
}

// SQLConditionPlanner interface for condition planners.
type SQLConditionPlanner interface {
	ToCondition(ctx *shared.PlannerContext) (sql.SQLCondition, error)
	// NeedsAttributeJoin returns true if this condition requires the attributes table
	NeedsAttributeJoin() bool
	// GetAttributeKeys returns the attribute keys this condition references
	GetAttributeKeys() []string
}

// TrueConditionPlanner always returns true.
type TrueConditionPlanner struct{}

func (t *TrueConditionPlanner) ToCondition(ctx *shared.PlannerContext) (sql.SQLCondition, error) {
	return sql.Eq(sql.NewIntVal(1), sql.NewIntVal(1)), nil
}
func (t *TrueConditionPlanner) NeedsAttributeJoin() bool  { return false }
func (t *TrueConditionPlanner) GetAttributeKeys() []string { return nil }

// FalseConditionPlanner always returns false.
type FalseConditionPlanner struct{}

func (f *FalseConditionPlanner) ToCondition(ctx *shared.PlannerContext) (sql.SQLCondition, error) {
	return sql.Eq(sql.NewIntVal(1), sql.NewIntVal(0)), nil
}
func (f *FalseConditionPlanner) NeedsAttributeJoin() bool  { return false }
func (f *FalseConditionPlanner) GetAttributeKeys() []string { return nil }

// BinaryConditionPlanner handles AND/OR conditions.
type BinaryConditionPlanner struct {
	Left  SQLConditionPlanner
	Right SQLConditionPlanner
	Op    string // "AND" or "OR"
}

func (b *BinaryConditionPlanner) ToCondition(ctx *shared.PlannerContext) (sql.SQLCondition, error) {
	left, err := b.Left.ToCondition(ctx)
	if err != nil {
		return nil, err
	}
	right, err := b.Right.ToCondition(ctx)
	if err != nil {
		return nil, err
	}

	if b.Op == "AND" {
		return sql.And(left, right), nil
	}
	return sql.Or(left, right), nil
}

func (b *BinaryConditionPlanner) NeedsAttributeJoin() bool {
	return b.Left.NeedsAttributeJoin() || b.Right.NeedsAttributeJoin()
}

func (b *BinaryConditionPlanner) GetAttributeKeys() []string {
	var keys []string
	keys = append(keys, b.Left.GetAttributeKeys()...)
	keys = append(keys, b.Right.GetAttributeKeys()...)
	return keys
}

// NotConditionPlanner negates a condition.
type NotConditionPlanner struct {
	Inner SQLConditionPlanner
}

func (n *NotConditionPlanner) ToCondition(ctx *shared.PlannerContext) (sql.SQLCondition, error) {
	inner, err := n.Inner.ToCondition(ctx)
	if err != nil {
		return nil, err
	}
	return sql.Not(inner), nil
}

func (n *NotConditionPlanner) NeedsAttributeJoin() bool {
	return n.Inner.NeedsAttributeJoin()
}

func (n *NotConditionPlanner) GetAttributeKeys() []string {
	return n.Inner.GetAttributeKeys()
}

// WrapperConditionPlanner wraps a SQLRequestPlanner as a condition.
type WrapperConditionPlanner struct {
	Planner shared.SQLRequestPlanner
}

func (w *WrapperConditionPlanner) ToCondition(ctx *shared.PlannerContext) (sql.SQLCondition, error) {
	// This should be handled differently - nested queries
	return sql.Eq(sql.NewIntVal(1), sql.NewIntVal(1)), nil
}

func (w *WrapperConditionPlanner) NeedsAttributeJoin() bool  { return false }
func (w *WrapperConditionPlanner) GetAttributeKeys() []string { return nil }
