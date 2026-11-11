package traceql_ir

import (
	"strings"

	traceql_parser "github.com/metrico/qryn/v5/reader/traceql/traceql_parser"
)

// FieldExpr is a field expression: the filter inside `{…}` and the argument
// of an aggregate.
type FieldExpr interface {
	isFieldExpr()
	String() string
}

// Attribute is an attribute or an intrinsic as written, e.g. `.a`,
// `resource.service.name`, `duration` or `span:name`.
type Attribute struct {
	Name string
}

// Static is a literal as written. A number is an integer or a float; strings
// keep their quotes.
type Static struct {
	Kind ValueKind
	Text string
}

// Unquote returns the content of a quoted string literal.
func (s *Static) Unquote() (string, error) {
	return (&traceql_parser.QuotedString{Str: s.Text}).Unquote()
}

// BinaryOp is `LHS Op RHS`: arithmetic, a comparison, a regex match, `&&` or `||`.
type BinaryOp struct {
	Op  string
	LHS FieldExpr
	RHS FieldExpr
}

// UnaryOp is `-Expr` or `!Expr`.
type UnaryOp struct {
	Op   string
	Expr FieldExpr
}

// NilTest is `Expr != nil` when Exists, else `Expr = nil`.
type NilTest struct {
	Expr   FieldExpr
	Exists bool
}

func (*Attribute) isFieldExpr() {}
func (*Static) isFieldExpr()    {}
func (*BinaryOp) isFieldExpr()  {}
func (*UnaryOp) isFieldExpr()   {}
func (*NilTest) isFieldExpr()   {}

func (a *Attribute) String() string { return a.Name }

func (s *Static) String() string { return s.Text }

// String parenthesises operands other than attributes and literals.
func (o *BinaryOp) String() string {
	return wrapField(o.LHS) + " " + o.Op + " " + wrapField(o.RHS)
}

func (o *UnaryOp) String() string { return o.Op + wrapField(o.Expr) }

func (n *NilTest) String() string {
	if n.Exists {
		return wrapField(n.Expr) + " != nil"
	}
	return wrapField(n.Expr) + " = nil"
}

func wrapField(e FieldExpr) string {
	switch e.(type) {
	case *Attribute, *Static:
		return e.String()
	}
	return "(" + e.String() + ")"
}

// IsComparison reports whether op compares two values or matches a regex.
func IsComparison(op string) bool {
	switch op {
	case "=", "!=", "<", "<=", ">", ">=", "=~", "!~":
		return true
	}
	return false
}

// IsRegex reports whether op is a regex match.
func IsRegex(op string) bool { return op == "=~" || op == "!~" }

// IsLogical reports whether op is `&&` or `||`.
func IsLogical(op string) bool { return op == "&&" || op == "||" }

// IsArithmetic reports whether op is an arithmetic operator.
func IsArithmetic(op string) bool {
	switch op {
	case "+", "-", "*", "/", "%", "^":
		return true
	}
	return false
}

// AttributeKey returns the storage key of a scoped attribute: span and
// resource attributes share one namespace.
func AttributeKey(name string) (string, bool) {
	for _, scope := range []string{"span.", "resource.", "."} {
		if strings.HasPrefix(name, scope) {
			return name[len(scope):], true
		}
	}
	return "", false
}
