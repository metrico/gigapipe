package traceql_ir

import (
	"fmt"
	"regexp"
	"strings"
)

// FieldType is the type a field expression has before it reads a span.
type FieldType int

const (
	// TypeAttribute is an attribute, whose type is known only per span.
	TypeAttribute FieldType = iota
	TypeInt
	TypeFloat
	TypeDuration
	TypeString
	TypeBool
	TypeStatus
	TypeKind
	// TypeIdent is an unquoted word: a status or kind keyword, else a string.
	TypeIdent
)

// IsNumeric reports whether t is an int, a float or a duration.
func (t FieldType) IsNumeric() bool {
	return t == TypeInt || t == TypeFloat || t == TypeDuration
}

// intrinsicTypes are the types of the intrinsics TraceQL names.
var intrinsicTypes = map[string]FieldType{
	"duration": TypeDuration, "span:duration": TypeDuration,
	"name": TypeString, "span:name": TypeString,
	"status": TypeStatus, "span:status": TypeStatus,
	"kind": TypeKind, "span:kind": TypeKind,
	"statusMessage": TypeString, "span:statusMessage": TypeString,
	"rootName": TypeString, "trace:rootName": TypeString,
	"rootServiceName": TypeString, "trace:rootService": TypeString,
	"traceDuration": TypeDuration, "trace:duration": TypeDuration,
	NestedSetLeft: TypeInt, NestedSetRight: TypeInt, NestedSetParent: TypeInt,
	"childCount": TypeInt, "span:childCount": TypeInt,
	"span:id": TypeString, "span:parentID": TypeString, "trace:id": TypeString,
}

// IsIntrinsic reports whether name is an intrinsic rather than an attribute.
func IsIntrinsic(name string) bool {
	_, ok := intrinsicTypes[name]
	return ok || strings.Contains(name, ":")
}

// The nested-set intrinsics.
const (
	NestedSetLeft   = "nestedSetLeft"
	NestedSetRight  = "nestedSetRight"
	NestedSetParent = "nestedSetParent"
)

// IsNestedSet reports whether name is a nested-set intrinsic.
func IsNestedSet(name string) bool {
	return name == NestedSetLeft || name == NestedSetRight || name == NestedSetParent
}

// isFieldName reports whether an unquoted word names an attribute or an intrinsic.
func isFieldName(name string) bool {
	_, scoped := AttributeKey(name)
	return scoped || IsIntrinsic(name)
}

// EnumKeywords are the keywords status and kind compare with.
var EnumKeywords = map[FieldType][]string{
	TypeStatus: {"error", "ok", "unset"},
	TypeKind:   {"unspecified", "internal", "server", "client", "producer", "consumer"},
}

// TypeOf is the type of e before it reads a span: an arithmetic result has
// the type of its first operand that is not an attribute, and is a float
// when every operand is an attribute.
func TypeOf(e FieldExpr) FieldType {
	switch e := e.(type) {
	case *Attribute:
		if t, ok := intrinsicTypes[e.Name]; ok {
			return t
		}
		return TypeAttribute
	case *Static:
		switch e.Kind {
		case ValueNumber:
			if strings.ContainsAny(e.Text, ".eE") {
				return TypeFloat
			}
			return TypeInt
		case ValueDuration:
			return TypeDuration
		case ValueString:
			return TypeString
		case ValueBool:
			return TypeBool
		}
		return TypeIdent
	case *BinaryOp:
		if !IsArithmetic(e.Op) {
			return TypeBool
		}
		if t := TypeOf(e.LHS); t != TypeAttribute {
			return t
		}
		return arithmeticType(TypeOf(e.RHS))
	case *UnaryOp:
		if e.Op == "!" {
			return TypeBool
		}
		return arithmeticType(TypeOf(e.Expr))
	}
	return TypeBool
}

// arithmeticType is the type of an arithmetic result whose operand has type t.
func arithmeticType(t FieldType) FieldType {
	if t == TypeAttribute {
		return TypeFloat
	}
	return t
}

// ReferencesSpan reports whether e reads an attribute or an intrinsic.
func ReferencesSpan(e FieldExpr) bool {
	switch e := e.(type) {
	case *Attribute:
		return true
	case *BinaryOp:
		return ReferencesSpan(e.LHS) || ReferencesSpan(e.RHS)
	case *UnaryOp:
		return ReferencesSpan(e.Expr)
	case *NilTest:
		return ReferencesSpan(e.Expr)
	}
	return false
}

// CheckFilter validates the field expression of a spanset filter, which must
// resolve to a boolean.
func CheckFilter(e FieldExpr) error {
	if err := checkField(e); err != nil {
		return err
	}
	if t := TypeOf(e); t != TypeAttribute && t != TypeBool {
		return fmt.Errorf("span filter field expressions must resolve to a boolean: { %s }", e)
	}
	return nil
}

// CheckAggregate validates the argument of an aggregate, which must resolve
// to a number and read the span.
func CheckAggregate(a *Aggregate) error {
	if a.Arg == nil {
		return nil
	}
	if err := checkField(a.Arg); err != nil {
		return err
	}
	if t := TypeOf(a.Arg); t != TypeAttribute && !t.IsNumeric() {
		return fmt.Errorf("aggregate field expressions must resolve to a number type: %s", a)
	}
	if !ReferencesSpan(a.Arg) {
		return fmt.Errorf("aggregate field expressions must reference the span: %s", a)
	}
	return nil
}

// CheckGroupBy validates the expression of a by() stage, which must read the span.
func CheckGroupBy(g *GroupBy) error {
	if !ReferencesSpan(g.Expr) {
		return fmt.Errorf("grouping field expressions must reference the span: %s", g)
	}
	return checkField(g.Expr)
}

func checkField(e FieldExpr) error {
	switch e := e.(type) {
	case *BinaryOp:
		return checkBinary(e)
	case *UnaryOp:
		if err := checkField(e.Expr); err != nil {
			return err
		}
		t := TypeOf(e.Expr)
		valid := t == TypeAttribute || (e.Op == "-" && t.IsNumeric()) || (e.Op == "!" && t == TypeBool)
		if !valid {
			return fmt.Errorf("illegal operation for the given type: %s", e)
		}
	case *NilTest:
		if err := checkField(e.Expr); err != nil {
			return err
		}
		if attr, ok := e.Expr.(*Attribute); ok && !e.Exists {
			switch {
			case IsIntrinsic(attr.Name):
				return fmt.Errorf("%s=nil is not valid because intrinsics cannot be nil", attr)
			case attr.Name == "resource.service.name":
				return fmt.Errorf("%s=nil is not valid because resource.service.name cannot be nil", attr)
			}
		}
	}
	return nil
}

func checkBinary(e *BinaryOp) error {
	if err := checkField(e.LHS); err != nil {
		return err
	}
	if err := checkField(e.RHS); err != nil {
		return err
	}
	lt, rt := TypeOf(e.LHS), TypeOf(e.RHS)
	if !matchingTypes(lt, rt) {
		return fmt.Errorf("binary operations must operate on the same type: %s", e)
	}
	if !binaryTypeValid(e.Op, lt) || !binaryTypeValid(e.Op, rt) {
		return fmt.Errorf("illegal operation for the given types: %s", e)
	}
	for _, side := range [][2]FieldExpr{{e.LHS, e.RHS}, {e.RHS, e.LHS}} {
		if s, ok := side[0].(*Static); ok && s.Kind == ValueIdent && !ReferencesSpan(side[1]) {
			return fmt.Errorf("unknown identifier: %s", s)
		}
	}
	if IsRegex(e.Op) {
		re, ok := e.RHS.(*Static)
		if !ok || re.Kind != ValueString {
			return fmt.Errorf("invalid type for =~ or !~: %s", e.RHS)
		}
		text, err := re.Unquote()
		if err != nil {
			return err
		}
		if _, err := regexp.Compile(text); err != nil {
			return fmt.Errorf("invalid regex: %s", e.RHS)
		}
	}
	for _, side := range [][2]FieldExpr{{e.LHS, e.RHS}, {e.RHS, e.LHS}} {
		if keywords, ok := EnumKeywords[TypeOf(side[0])]; ok && !isKeyword(side[1], keywords) {
			return fmt.Errorf("%s compares only with = or != against %s: %s",
				side[0], strings.Join(keywords, ", "), e)
		}
	}
	return nil
}

func isKeyword(e FieldExpr, keywords []string) bool {
	s, ok := e.(*Static)
	if !ok || s.Kind != ValueIdent {
		return false
	}
	for _, k := range keywords {
		if s.Text == k {
			return true
		}
	}
	return false
}

// matchingTypes reports whether two operand types can meet: the same type,
// two numbers, an attribute, or an unquoted word with a string or an enum.
func matchingTypes(a, b FieldType) bool {
	switch {
	case a == b, a.IsNumeric() && b.IsNumeric(), a == TypeAttribute, b == TypeAttribute:
		return true
	case a == TypeIdent:
		return b == TypeString || b == TypeStatus || b == TypeKind
	case b == TypeIdent:
		return a == TypeString || a == TypeStatus || a == TypeKind
	}
	return false
}

func binaryTypeValid(op string, t FieldType) bool {
	switch t {
	case TypeAttribute:
		return true
	case TypeBool:
		return IsLogical(op) || op == "=" || op == "!="
	case TypeInt, TypeFloat, TypeDuration:
		return IsArithmetic(op) || (IsComparison(op) && !IsRegex(op))
	case TypeString, TypeIdent:
		return IsComparison(op)
	case TypeStatus, TypeKind:
		return op == "=" || op == "!="
	}
	return false
}
