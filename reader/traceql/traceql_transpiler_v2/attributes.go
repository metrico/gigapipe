package traceql_transpiler_v2

import (
	"fmt"
	"strconv"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v4/reader/traceql/tempo"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

// planScopedAttributeComparison handles comparisons with scoped attributes (span., resource., .).
func (p *TempoPlanner) planScopedAttributeComparison(attr tempo.Attribute, op tempo.Operator, value tempo.FieldExpression) (SQLConditionPlanner, error) {
	key := p.getAttributeKey(attr)

	static, ok := value.(tempo.Static)
	if !ok {
		return nil, fmt.Errorf("attribute comparison requires static value, got %T", value)
	}

	// Determine value type and create appropriate planner
	switch static.Type {
	case tempo.TypeString:
		strVal := static.EncodeToString(false)
		return &AttributeStringComparisonPlanner{
			Key:   key,
			Op:    op,
			Value: strVal,
		}, nil

	case tempo.TypeInt:
		intVal, _ := static.Int()
		return &AttributeNumericComparisonPlanner{
			Key:   key,
			Op:    op,
			Value: float64(intVal),
		}, nil

	case tempo.TypeFloat:
		floatVal := static.Float()
		return &AttributeNumericComparisonPlanner{
			Key:   key,
			Op:    op,
			Value: floatVal,
		}, nil

	case tempo.TypeBoolean:
		boolVal, _ := static.Bool()
		strVal := "false"
		if boolVal {
			strVal = "true"
		}
		return &AttributeStringComparisonPlanner{
			Key:   key,
			Op:    op,
			Value: strVal,
		}, nil

	case tempo.TypeDuration:
		dur, _ := static.Duration()
		return &AttributeNumericComparisonPlanner{
			Key:   key,
			Op:    op,
			Value: float64(dur.Nanoseconds()),
		}, nil

	case tempo.TypeStatus:
		status, _ := static.Status()
		return &AttributeStringComparisonPlanner{
			Key:   key,
			Op:    op,
			Value: statusToString(status),
		}, nil

	case tempo.TypeKind:
		kind, _ := static.Kind()
		return &AttributeStringComparisonPlanner{
			Key:   key,
			Op:    op,
			Value: kindToString(kind),
		}, nil

	default:
		// Default to string comparison
		strVal := static.EncodeToString(false)
		return &AttributeStringComparisonPlanner{
			Key:   key,
			Op:    op,
			Value: strVal,
		}, nil
	}
}

// getAttributeKey extracts the key from an attribute, handling scope prefixes.
func (p *TempoPlanner) getAttributeKey(attr tempo.Attribute) string {
	// The key is already in the Name field
	// Scope is handled by the Scope field
	return attr.Name
}

// AttributeStringComparisonPlanner handles string comparisons with attributes.
type AttributeStringComparisonPlanner struct {
	Key   string
	Op    tempo.Operator
	Value string
}

func (a *AttributeStringComparisonPlanner) ToCondition(ctx *shared.PlannerContext) (sql.SQLCondition, error) {
	switch a.Op {
	case tempo.OpEqual:
		return sql.And(
			sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(a.Key)),
			sql.Eq(sql.NewRawObject("val"), sql.NewStringVal(a.Value)),
		), nil
	case tempo.OpNotEqual:
		return sql.And(
			sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(a.Key)),
			sql.Neq(sql.NewRawObject("val"), sql.NewStringVal(a.Value)),
		), nil
	case tempo.OpRegex:
		return sql.And(
			sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(a.Key)),
			sql.Eq(&matchRe{sql.NewRawObject("val"), a.Value}, sql.NewIntVal(1)),
		), nil
	case tempo.OpNotRegex:
		return sql.And(
			sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(a.Key)),
			sql.Eq(&matchRe{sql.NewRawObject("val"), a.Value}, sql.NewIntVal(0)),
		), nil
	default:
		return nil, fmt.Errorf("unsupported operator for string attribute: %v", a.Op)
	}
}

func (a *AttributeStringComparisonPlanner) NeedsAttributeJoin() bool   { return true }
func (a *AttributeStringComparisonPlanner) GetAttributeKeys() []string { return []string{a.Key} }

// AttributeNumericComparisonPlanner handles numeric comparisons with attributes.
type AttributeNumericComparisonPlanner struct {
	Key   string
	Op    tempo.Operator
	Value float64
}

func (a *AttributeNumericComparisonPlanner) ToCondition(ctx *shared.PlannerContext) (sql.SQLCondition, error) {
	compFn := getComparisonFn(a.Op)
	if compFn == nil {
		return nil, fmt.Errorf("unsupported operator for numeric attribute: %v", a.Op)
	}

	return sql.And(
		sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(a.Key)),
		sql.Eq(sql.NewRawObject("isNotNull(toFloat64OrNull(val))"), sql.NewIntVal(1)),
		compFn(sql.NewRawObject("toFloat64OrZero(val)"), sql.NewFloatVal(a.Value)),
	), nil
}

func (a *AttributeNumericComparisonPlanner) NeedsAttributeJoin() bool   { return true }
func (a *AttributeNumericComparisonPlanner) GetAttributeKeys() []string { return []string{a.Key} }

// AttributeExistsPlanner checks if an attribute exists.
type AttributeExistsPlanner struct {
	Key string
}

func (a *AttributeExistsPlanner) ToCondition(ctx *shared.PlannerContext) (sql.SQLCondition, error) {
	return sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(a.Key)), nil
}

func (a *AttributeExistsPlanner) NeedsAttributeJoin() bool   { return true }
func (a *AttributeExistsPlanner) GetAttributeKeys() []string { return []string{a.Key} }

// Static helpers

// staticToSQLValue converts a tempo.Static to a sql.SQLObject value.
func staticToSQLValue(s tempo.Static) sql.SQLObject {
	switch s.Type {
	case tempo.TypeString:
		return sql.NewStringVal(s.EncodeToString(false))
	case tempo.TypeInt:
		intVal, _ := s.Int()
		return sql.NewIntVal(int64(intVal))
	case tempo.TypeFloat:
		return sql.NewFloatVal(s.Float())
	case tempo.TypeBoolean:
		boolVal, _ := s.Bool()
		if boolVal {
			return sql.NewIntVal(1)
		}
		return sql.NewIntVal(0)
	case tempo.TypeDuration:
		dur, _ := s.Duration()
		return sql.NewIntVal(dur.Nanoseconds())
	default:
		return sql.NewStringVal(s.EncodeToString(false))
	}
}

// parseNumericValue parses a numeric value from string.
func parseNumericValue(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}
