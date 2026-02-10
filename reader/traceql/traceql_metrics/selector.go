package traceql_metrics

import (
	"fmt"
	"strings"

	"github.com/metrico/qryn/v4/reader/traceql/tempo"
)

// SelectorConditions holds parsed conditions from a TraceQL selector.
type SelectorConditions struct {
	Conditions []string // SQL WHERE clause fragments
	NeedsJoin  bool     // Whether the query needs to JOIN with attributes table
}

// BuildSelectorConditions extracts SQL conditions from a Tempo Pipeline.
// Returns conditions that can be added to a WHERE clause.
func BuildSelectorConditions(pipeline tempo.Pipeline) (*SelectorConditions, error) {
	result := &SelectorConditions{}

	for _, elem := range pipeline.Elements {
		switch e := elem.(type) {
		case *tempo.SpansetFilter:
			if e.Expression == nil {
				continue
			}
			cond, needsJoin, err := buildFieldExpression(e.Expression)
			if err != nil {
				return nil, err
			}
			if cond != "" {
				result.Conditions = append(result.Conditions, cond)
			}
			if needsJoin {
				result.NeedsJoin = true
			}
		case tempo.SpansetOperation:
			// Handle spanset operations recursively
			leftCond, leftJoin, err := buildSpansetExpression(e.LHS)
			if err != nil {
				return nil, err
			}
			rightCond, rightJoin, err := buildSpansetExpression(e.RHS)
			if err != nil {
				return nil, err
			}
			if leftCond != "" {
				result.Conditions = append(result.Conditions, leftCond)
			}
			if rightCond != "" {
				result.Conditions = append(result.Conditions, rightCond)
			}
			result.NeedsJoin = result.NeedsJoin || leftJoin || rightJoin
		}
	}

	return result, nil
}

func buildSpansetExpression(expr tempo.SpansetExpression) (string, bool, error) {
	switch e := expr.(type) {
	case *tempo.SpansetFilter:
		if e.Expression == nil {
			return "", false, nil
		}
		return buildFieldExpression(e.Expression)
	case tempo.Pipeline:
		conds, err := BuildSelectorConditions(e)
		if err != nil {
			return "", false, err
		}
		if len(conds.Conditions) == 0 {
			return "", false, nil
		}
		return "(" + strings.Join(conds.Conditions, " AND ") + ")", conds.NeedsJoin, nil
	default:
		return "", false, nil
	}
}

// buildFieldExpression converts a FieldExpression to SQL condition string.
func buildFieldExpression(expr tempo.FieldExpression) (string, bool, error) {
	switch e := expr.(type) {
	case *tempo.BinaryOperation:
		return buildBinaryOperation(e)
	case tempo.UnaryOperation:
		return buildUnaryOperation(e)
	case tempo.Attribute:
		return buildAttributeExistence(e)
	case tempo.Static:
		return buildStaticCondition(e)
	default:
		return "", false, nil
	}
}

func buildBinaryOperation(op *tempo.BinaryOperation) (string, bool, error) {
	switch op.Op {
	case tempo.OpAnd:
		left, leftJoin, err := buildFieldExpression(op.LHS)
		if err != nil {
			return "", false, err
		}
		right, rightJoin, err := buildFieldExpression(op.RHS)
		if err != nil {
			return "", false, err
		}
		if left == "" && right == "" {
			return "", false, nil
		}
		if left == "" {
			return right, rightJoin, nil
		}
		if right == "" {
			return left, leftJoin, nil
		}
		return fmt.Sprintf("(%s AND %s)", left, right), leftJoin || rightJoin, nil

	case tempo.OpOr:
		left, leftJoin, err := buildFieldExpression(op.LHS)
		if err != nil {
			return "", false, err
		}
		right, rightJoin, err := buildFieldExpression(op.RHS)
		if err != nil {
			return "", false, err
		}
		if left == "" || right == "" {
			return "", false, nil
		}
		return fmt.Sprintf("(%s OR %s)", left, right), leftJoin || rightJoin, nil

	case tempo.OpEqual, tempo.OpNotEqual, tempo.OpGreater, tempo.OpGreaterEqual,
		tempo.OpLess, tempo.OpLessEqual, tempo.OpRegex, tempo.OpNotRegex:
		return buildComparisonOperation(op)

	default:
		return "", false, nil
	}
}

func buildComparisonOperation(op *tempo.BinaryOperation) (string, bool, error) {
	attr, isAttr := op.LHS.(tempo.Attribute)
	if !isAttr {
		if rAttr, ok := op.RHS.(tempo.Attribute); ok {
			attr = rAttr
			return buildAttributeComparison(attr, op.Op, op.LHS)
		}
		return "", false, nil
	}
	return buildAttributeComparison(attr, op.Op, op.RHS)
}

func buildAttributeComparison(attr tempo.Attribute, op tempo.Operator, value tempo.FieldExpression) (string, bool, error) {
	// Handle intrinsics
	if attr.Intrinsic != tempo.IntrinsicNone {
		return buildIntrinsicComparison(attr, op, value)
	}

	// Handle regular attributes - these need a JOIN with the attributes table
	static, ok := value.(tempo.Static)
	if !ok {
		return "", false, nil
	}

	key := getAttributeKey(attr)
	sqlOp := getSQLOperator(op)
	if sqlOp == "" {
		return "", false, nil
	}

	// For attributes, we query the traces_attrs table
	// The condition will be applied to the joined attrs table
	switch static.Type {
	case tempo.TypeString:
		strVal := escapeString(static.EncodeToString(false))
		if op == tempo.OpRegex {
			return fmt.Sprintf("(a.key = '%s' AND match(a.val, '%s'))", key, strVal), true, nil
		}
		if op == tempo.OpNotRegex {
			return fmt.Sprintf("(a.key = '%s' AND NOT match(a.val, '%s'))", key, strVal), true, nil
		}
		return fmt.Sprintf("(a.key = '%s' AND a.val %s '%s')", key, sqlOp, strVal), true, nil

	case tempo.TypeInt:
		intVal, _ := static.Int()
		return fmt.Sprintf("(a.key = '%s' AND toFloat64OrZero(a.val) %s %d)", key, sqlOp, intVal), true, nil

	case tempo.TypeFloat:
		floatVal := static.Float()
		return fmt.Sprintf("(a.key = '%s' AND toFloat64OrZero(a.val) %s %f)", key, sqlOp, floatVal), true, nil

	case tempo.TypeStatus:
		status, _ := static.Status()
		statusStr := statusToString(status)
		return fmt.Sprintf("(a.key = '%s' AND a.val %s '%s')", key, sqlOp, statusStr), true, nil

	default:
		strVal := escapeString(static.EncodeToString(false))
		return fmt.Sprintf("(a.key = '%s' AND a.val %s '%s')", key, sqlOp, strVal), true, nil
	}
}

func buildIntrinsicComparison(attr tempo.Attribute, op tempo.Operator, value tempo.FieldExpression) (string, bool, error) {
	static, ok := value.(tempo.Static)
	if !ok {
		return "", false, nil
	}

	sqlOp := getSQLOperator(op)
	if sqlOp == "" {
		return "", false, nil
	}

	switch attr.Intrinsic {
	case tempo.IntrinsicStatus:
		// status is stored as attribute in some schemas, or as column
		status, _ := static.Status()
		statusStr := statusToString(status)
		// Try both: status_code column and attribute
		return fmt.Sprintf("(a.key = 'status' AND a.val %s '%s')", sqlOp, statusStr), true, nil

	case tempo.IntrinsicDuration:
		// duration_ns column in the traces table
		dur, ok := static.Duration()
		if !ok {
			if intVal, ok := static.Int(); ok {
				// Assume nanoseconds if int
				return fmt.Sprintf("duration_ns %s %d", sqlOp, intVal), false, nil
			}
			return "", false, nil
		}
		return fmt.Sprintf("duration_ns %s %d", sqlOp, dur.Nanoseconds()), false, nil

	case tempo.IntrinsicName:
		strVal := escapeString(static.EncodeToString(false))
		if op == tempo.OpRegex {
			return fmt.Sprintf("match(name, '%s')", strVal), false, nil
		}
		if op == tempo.OpNotRegex {
			return fmt.Sprintf("NOT match(name, '%s')", strVal), false, nil
		}
		return fmt.Sprintf("name %s '%s'", sqlOp, strVal), false, nil

	case tempo.IntrinsicNestedSetParent:
		// nestedSetParent < 0 means root span (no parent)
		// nestedSetParent >= 0 means non-root span (has parent)
		intVal, _ := static.Int()
		if intVal == 0 && op == tempo.OpLess {
			// nestedSetParent < 0 → root span
			return "parent_id = ''", false, nil
		}
		if intVal == 0 && op == tempo.OpLessEqual {
			// nestedSetParent <= 0 → root span (same as < 0 for this semantic)
			return "parent_id = ''", false, nil
		}
		if intVal == 0 && op == tempo.OpGreaterEqual {
			// nestedSetParent >= 0 → non-root span (has parent)
			return "parent_id != ''", false, nil
		}
		if intVal == 0 && op == tempo.OpGreater {
			// nestedSetParent > 0 → non-root span (has parent)
			return "parent_id != ''", false, nil
		}
		if intVal < 0 && (op == tempo.OpLess || op == tempo.OpLessEqual) {
			return "parent_id = ''", false, nil
		}
		if intVal >= 0 && (op == tempo.OpGreater || op == tempo.OpGreaterEqual) {
			return "parent_id != ''", false, nil
		}
		return "", false, nil

	default:
		return "", false, nil
	}
}

func buildUnaryOperation(op tempo.UnaryOperation) (string, bool, error) {
	switch op.Op {
	case tempo.OpNot:
		inner, needsJoin, err := buildFieldExpression(op.Expression)
		if err != nil {
			return "", false, err
		}
		if inner == "" {
			return "", false, nil
		}
		return fmt.Sprintf("NOT (%s)", inner), needsJoin, nil
	default:
		return "", false, nil
	}
}

func buildAttributeExistence(attr tempo.Attribute) (string, bool, error) {
	if attr.Intrinsic != tempo.IntrinsicNone {
		// Intrinsics always exist
		return "", false, nil
	}
	key := getAttributeKey(attr)
	return fmt.Sprintf("a.key = '%s'", key), true, nil
}

func buildStaticCondition(static tempo.Static) (string, bool, error) {
	if static.Type == tempo.TypeBoolean {
		if b, ok := static.Bool(); ok {
			if b {
				// true is a no-op, return empty
				return "", false, nil
			}
			return "1=0", false, nil
		}
	}
	return "", false, nil
}

// Helper functions

func getAttributeKey(attr tempo.Attribute) string {
	return escapeString(attr.Name)
}

func getSQLOperator(op tempo.Operator) string {
	switch op {
	case tempo.OpEqual:
		return "="
	case tempo.OpNotEqual:
		return "!="
	case tempo.OpGreater:
		return ">"
	case tempo.OpGreaterEqual:
		return ">="
	case tempo.OpLess:
		return "<"
	case tempo.OpLessEqual:
		return "<="
	default:
		return ""
	}
}

func statusToString(status tempo.Status) string {
	switch status {
	case tempo.StatusOk:
		return "ok"
	case tempo.StatusError:
		return "error"
	case tempo.StatusUnset:
		return "unset"
	default:
		return "unset"
	}
}

// escapeString escapes a string for safe use in ClickHouse SQL queries.
// It handles single quotes, backslashes, and null bytes.
func escapeString(s string) string {
	// Escape backslashes first, then single quotes
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "'", "\\'")
	// Remove null bytes which can cause issues
	s = strings.ReplaceAll(s, "\x00", "")
	return s
}
