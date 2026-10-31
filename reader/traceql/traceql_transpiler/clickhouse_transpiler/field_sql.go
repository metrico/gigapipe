package clickhouse_transpiler

import (
	"fmt"
	"slices"
	"strconv"
	"time"

	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
)

// fieldRef is what an attribute or intrinsic reads: the attribute key, the
// span duration when duration is set, or the stored nested-set column.
type fieldRef struct {
	key       string
	duration  bool
	nestedSet nestedSetCol
}

// fieldRefOf maps an attribute or intrinsic to what it reads. An unscoped
// word that is no intrinsic is an attribute key.
func fieldRefOf(name string) (fieldRef, error) {
	if key, ok := traceql_ir.AttributeKey(name); ok {
		return fieldRef{key: key}, nil
	}
	switch name {
	case "duration", "span:duration":
		return fieldRef{duration: true}, nil
	case "name", "span:name", "rootName":
		return fieldRef{key: "name"}, nil
	case "status", "kind", "statusMessage":
		return fieldRef{key: name}, nil
	case "rootServiceName":
		return fieldRef{key: "service.name"}, nil
	}
	if col, ok := nestedSetCols[name]; ok {
		return fieldRef{nestedSet: col}, nil
	}
	if traceql_ir.IsIntrinsic(name) {
		return fieldRef{}, fmt.Errorf("unsupported attribute %s", name)
	}
	return fieldRef{key: name}, nil
}

// pivotOf plans the pivot of what e reads: its attribute keys, each once
// in order, its nested-set columns and whether it holds a root filter.
func pivotOf(e traceql_ir.FieldExpr) (*fieldPivot, error) {
	p := &fieldPivot{}
	var walk func(e traceql_ir.FieldExpr) error
	walk = func(e traceql_ir.FieldExpr) error {
		switch e := e.(type) {
		case *traceql_ir.Attribute:
			ref, err := fieldRefOf(e.Name)
			if err != nil {
				return err
			}
			switch {
			case ref.nestedSet != "":
				if !slices.Contains(p.nestedSet, ref.nestedSet) {
					p.nestedSet = append(p.nestedSet, ref.nestedSet)
				}
			case !ref.duration && !containsKey(p.keys, ref.key):
				p.keys = append(p.keys, ref.key)
			}
		case *traceql_ir.BinaryOp:
			if isRootFilter(e) {
				p.root = true
				return nil
			}
			if err := walk(e.LHS); err != nil {
				return err
			}
			return walk(e.RHS)
		case *traceql_ir.UnaryOp:
			return walk(e.Expr)
		case *traceql_ir.NilTest:
			return walk(e.Expr)
		}
		return nil
	}
	return p, walk(e)
}

// isRootFilter reports whether e is `nestedSetParent < 0` or
// `nestedSetParent = -1`, which hold for the spans without a parent id.
func isRootFilter(e *traceql_ir.BinaryOp) bool {
	attr, ok := e.LHS.(*traceql_ir.Attribute)
	if !ok || attr.Name != traceql_ir.NestedSetParent {
		return false
	}
	lit, ok := e.RHS.(*traceql_ir.Static)
	if !ok || lit.Kind != traceql_ir.ValueNumber {
		return false
	}
	v, err := strconv.ParseFloat(lit.Text, 64)
	if err != nil {
		return false
	}
	return e.Op == "<" && v == 0 || e.Op == "=" && v == -1
}

// fieldSQL compiles checked field expressions to ClickHouse SQL under the nil
// rule: a missing attribute, a failed cast and an integer division or modulo
// by zero are NULL, NULL propagates through arithmetic, and a comparison with
// NULL is false. An attribute is cast to the type of the operand it meets.
type fieldSQL struct {
	// attr is the column holding the value of an attribute key, NULL when
	// the span lacks it.
	attr func(key string) string
	// duration is the column holding the span duration in nanoseconds.
	duration string
	// nestedSet is the pivot column of a stored nested-set column, 0 for a
	// span without one.
	nestedSet func(col nestedSetCol) string
	// root is the column that is 1 for a span without a parent id.
	root string
}

// number is a numeric SQL value: val is a Nullable(Float64), isInt whether
// it is an integer.
type number struct {
	val   string
	isInt string
}

// domain is the type a comparison casts its operands to.
type domain int

const (
	domainAny domain = iota
	domainNumber
	domainString
	domainBool
)

// filter is the SQL condition of a spanset filter: true only when e is true.
func (c *fieldSQL) filter(e traceql_ir.FieldExpr) (string, error) {
	b, err := c.boolean(e)
	if err != nil {
		return "", err
	}
	return "ifNull(" + b + ", 0)", nil
}

// boolean compiles e to a Nullable(UInt8), NULL when e is not a boolean.
func (c *fieldSQL) boolean(e traceql_ir.FieldExpr) (string, error) {
	switch e := e.(type) {
	case *traceql_ir.Attribute:
		v, err := c.text(e)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("multiIf(%s = 'true', 1, %s = 'false', 0, NULL)", v, v), nil
	case *traceql_ir.Static:
		if e.Kind == traceql_ir.ValueBool {
			if e.Text == "true" {
				return "1", nil
			}
			return "0", nil
		}
	case *traceql_ir.BinaryOp:
		if traceql_ir.IsLogical(e.Op) {
			l, err := c.boolean(e.LHS)
			if err != nil {
				return "", err
			}
			r, err := c.boolean(e.RHS)
			if err != nil {
				return "", err
			}
			op := map[string]string{"&&": "AND", "||": "OR"}[e.Op]
			return fmt.Sprintf("(ifNull(%s, 0) %s ifNull(%s, 0))", l, op, r), nil
		}
		if traceql_ir.IsComparison(e.Op) {
			return c.comparison(e)
		}
	case *traceql_ir.UnaryOp:
		if e.Op == "!" {
			b, err := c.boolean(e.Expr)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("ifNull(NOT %s, 0)", b), nil
		}
	case *traceql_ir.NilTest:
		v, err := c.value(e.Expr)
		if err != nil {
			return "", err
		}
		if e.Exists {
			return fmt.Sprintf("isNotNull(%s)", v), nil
		}
		return fmt.Sprintf("isNull(%s)", v), nil
	}
	return "", fmt.Errorf("expected a boolean: %s", e)
}

// value compiles e to SQL of its own type, NULL when e is nil.
func (c *fieldSQL) value(e traceql_ir.FieldExpr) (string, error) {
	switch domainOf(e) {
	case domainNumber:
		n, err := c.number(e)
		return n.val, err
	case domainBool:
		return c.boolean(e)
	}
	return c.text(e)
}

// number compiles e to a number.
func (c *fieldSQL) number(e traceql_ir.FieldExpr) (number, error) {
	switch e := e.(type) {
	case *traceql_ir.Attribute:
		ref, err := fieldRefOf(e.Name)
		if err != nil {
			return number{}, err
		}
		if ref.duration {
			return number{val: "toFloat64(" + c.duration + ")", isInt: "0"}, nil
		}
		if ref.nestedSet != "" {
			return number{val: "toFloat64(" + c.nestedSet(ref.nestedSet) + ")", isInt: "1"}, nil
		}
		v := c.attr(ref.key)
		return number{val: "toFloat64OrNull(" + v + ")", isInt: "isNotNull(toInt64OrNull(" + v + "))"}, nil
	case *traceql_ir.Static:
		return staticNumber(e)
	case *traceql_ir.UnaryOp:
		if e.Op == "-" {
			n, err := c.number(e.Expr)
			return number{val: "-(" + n.val + ")", isInt: n.isInt}, err
		}
	case *traceql_ir.BinaryOp:
		if traceql_ir.IsArithmetic(e.Op) {
			return c.arithmetic(e)
		}
	}
	return number{}, fmt.Errorf("expected a number: %s", e)
}

// arithmetic compiles `l op r`. Integers divide and take the modulo as
// integers, nil by zero; floats follow IEEE 754.
func (c *fieldSQL) arithmetic(e *traceql_ir.BinaryOp) (number, error) {
	l, err := c.number(e.LHS)
	if err != nil {
		return number{}, err
	}
	r, err := c.number(e.RHS)
	if err != nil {
		return number{}, err
	}
	isInt := fmt.Sprintf("(%s AND %s)", l.isInt, r.isInt)
	var val string
	switch e.Op {
	case "+", "-", "*":
		val = fmt.Sprintf("(%s %s %s)", l.val, e.Op, r.val)
	case "/":
		val = fmt.Sprintf("if(%s, if(%s = 0, NULL, trunc(%s / %s)), %s / %s)", isInt, r.val, l.val, r.val, l.val, r.val)
	case "%":
		val = fmt.Sprintf("if(%s, if(%s = 0, NULL, %s %% %s), %s %% %s)", isInt, r.val, l.val, r.val, l.val, r.val)
	case "^":
		val = fmt.Sprintf("pow(%s, %s)", l.val, r.val)
	}
	return number{val: val, isInt: isInt}, nil
}

// text compiles e to a Nullable(String).
func (c *fieldSQL) text(e traceql_ir.FieldExpr) (string, error) {
	switch e := e.(type) {
	case *traceql_ir.Attribute:
		ref, err := fieldRefOf(e.Name)
		if err != nil {
			return "", err
		}
		if ref.duration {
			return "toString(" + c.duration + ")", nil
		}
		if ref.nestedSet != "" {
			return "toString(" + c.nestedSet(ref.nestedSet) + ")", nil
		}
		return c.attr(ref.key), nil
	case *traceql_ir.Static:
		switch e.Kind {
		case traceql_ir.ValueString:
			s, err := e.Unquote()
			return sqlString(s), err
		case traceql_ir.ValueIdent:
			return sqlString(e.Text), nil
		}
	}
	return "", fmt.Errorf("expected a string: %s", e)
}

// comparison compiles `l op r` to a UInt8, false when either side is nil.
// Two attributes compare as numbers when both are numbers, as strings when
// neither is, and are nil otherwise.
func (c *fieldSQL) comparison(e *traceql_ir.BinaryOp) (string, error) {
	if isRootFilter(e) {
		return c.root, nil
	}
	d := domainOf(e.LHS)
	if d == domainAny {
		d = domainOf(e.RHS)
	}
	var (
		cmp string
		err error
	)
	switch d {
	case domainNumber:
		cmp, err = c.compareWith(e, func(x traceql_ir.FieldExpr) (string, error) {
			n, err := c.number(x)
			return n.val, err
		})
	case domainBool:
		cmp, err = c.compareWith(e, c.boolean)
	case domainString:
		if traceql_ir.IsRegex(e.Op) {
			cmp, err = c.regex(e)
		} else {
			cmp, err = c.compareWith(e, c.text)
		}
	default:
		cmp, err = c.compareAttributes(e)
	}
	if err != nil {
		return "", err
	}
	return "ifNull(" + cmp + ", 0)", nil
}

func (c *fieldSQL) compareWith(e *traceql_ir.BinaryOp, compile func(traceql_ir.FieldExpr) (string, error)) (string, error) {
	l, err := compile(e.LHS)
	if err != nil {
		return "", err
	}
	r, err := compile(e.RHS)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("(%s %s %s)", l, e.Op, r), nil
}

func (c *fieldSQL) regex(e *traceql_ir.BinaryOp) (string, error) {
	l, err := c.text(e.LHS)
	if err != nil {
		return "", err
	}
	r, err := c.text(e.RHS)
	if err != nil {
		return "", err
	}
	if e.Op == "!~" {
		return fmt.Sprintf("NOT match(%s, %s)", l, r), nil
	}
	return fmt.Sprintf("match(%s, %s)", l, r), nil
}

func (c *fieldSQL) compareAttributes(e *traceql_ir.BinaryOp) (string, error) {
	l, err := c.text(e.LHS)
	if err != nil {
		return "", err
	}
	r, err := c.text(e.RHS)
	if err != nil {
		return "", err
	}
	ln, rn := "toFloat64OrNull("+l+")", "toFloat64OrNull("+r+")"
	return fmt.Sprintf("multiIf(isNotNull(%s) AND isNotNull(%s), %s %s %s, isNull(%s) AND isNull(%s), %s %s %s, NULL)",
		ln, rn, ln, e.Op, rn, ln, rn, l, e.Op, r), nil
}

// domainOf is the type e has in SQL, or domainAny for an attribute whose
// type is known only per span.
func domainOf(e traceql_ir.FieldExpr) domain {
	switch traceql_ir.TypeOf(e) {
	case traceql_ir.TypeInt, traceql_ir.TypeFloat, traceql_ir.TypeDuration:
		return domainNumber
	case traceql_ir.TypeBool:
		return domainBool
	case traceql_ir.TypeAttribute:
		return domainAny
	}
	return domainString
}

// staticNumber is a number or duration literal; a duration is in nanoseconds.
func staticNumber(s *traceql_ir.Static) (number, error) {
	switch s.Kind {
	case traceql_ir.ValueDuration:
		d, err := time.ParseDuration(s.Text)
		if err != nil {
			return number{}, err
		}
		return number{val: floatLiteral(float64(d.Nanoseconds())), isInt: "0"}, nil
	case traceql_ir.ValueNumber:
		f, err := strconv.ParseFloat(s.Text, 64)
		if err != nil {
			return number{}, err
		}
		isInt := "0"
		if traceql_ir.TypeOf(s) == traceql_ir.TypeInt {
			isInt = "1"
		}
		return number{val: floatLiteral(f), isInt: isInt}, nil
	}
	return number{}, fmt.Errorf("expected a number: %s", s)
}
