package traceql_ir

import (
	"errors"
	"strings"

	traceql_parser "github.com/metrico/qryn/v5/reader/traceql/traceql_parser"
)

var errUnexpectedNil = errors.New("syntax error: nil compares only with = or !=")

// binaryPrecedence is the precedence of the binary field operators, lowest
// first. `^` is right-associative, the rest left-associative.
var binaryPrecedence = map[string]int{
	"&&": 1, "||": 1,
	"=": 2, "!=": 2, "<": 2, "<=": 2, ">": 2, ">=": 2, "=~": 2, "!~": 2,
	"+": 3, "-": 3,
	"*": 5, "/": 5, "%": 5,
	"^": 6,
}

// unaryPrecedence is the precedence of the unary operators: `-` binds like
// binary `-`, `!` between `+ -` and `* / %`. Each takes the following binary
// operations that bind tighter as its operand.
var unaryPrecedence = map[string]int{"-": 3, "!": 4}

// nilLiteral is `nil` before lowering folds it into a NilTest.
type nilLiteral struct{}

func (nilLiteral) isFieldExpr()   {}
func (nilLiteral) String() string { return "nil" }

func lowerOptionalField(e *traceql_parser.FieldExpr) (FieldExpr, error) {
	if e == nil {
		return nil, nil
	}
	return lowerField(e)
}

// lowerField groups a written field expression by precedence.
func lowerField(e *traceql_parser.FieldExpr) (FieldExpr, error) {
	l := &fieldLowering{operands: []traceql_parser.FieldOperand{e.Head}}
	for _, op := range e.Ops {
		l.ops = append(l.ops, op.Op)
		l.operands = append(l.operands, op.Operand)
	}
	res, err := l.expr(0)
	if err != nil {
		return nil, err
	}
	if _, ok := res.(nilLiteral); ok {
		return nil, errUnexpectedNil
	}
	return res, nil
}

// fieldLowering is a precedence climber over operands[i] ops[i] operands[i+1].
type fieldLowering struct {
	operands []traceql_parser.FieldOperand
	ops      []string
	// next is the operand to read; unary counts the unary operators of it
	// already read.
	next  int
	unary int
}

// expr reads an operand and the binary operations after it that bind at
// least as tight as minPrec.
func (l *fieldLowering) expr(minPrec int) (FieldExpr, error) {
	lhs, err := l.operand()
	if err != nil {
		return nil, err
	}
	for l.next < len(l.operands) {
		op := l.ops[l.next-1]
		prec := binaryPrecedence[op]
		if prec < minPrec {
			break
		}
		next := prec + 1
		if op == "^" {
			next = prec
		}
		rhs, err := l.expr(next)
		if err != nil {
			return nil, err
		}
		if lhs, err = newBinary(op, lhs, rhs); err != nil {
			return nil, err
		}
	}
	return lhs, nil
}

// operand reads the next unary operator with its operand, or the next atom.
func (l *fieldLowering) operand() (FieldExpr, error) {
	o := l.operands[l.next]
	if l.unary < len(o.Unary) {
		op := o.Unary[l.unary]
		l.unary++
		e, err := l.expr(unaryPrecedence[op] + 1)
		if err != nil {
			return nil, err
		}
		return newUnary(op, e)
	}
	l.next++
	l.unary = 0
	return lowerAtom(o.Atom)
}

func lowerAtom(a traceql_parser.FieldAtom) (FieldExpr, error) {
	switch {
	case a.Paren != nil:
		return lowerField(a.Paren)
	case a.Str != nil:
		return &Static{Kind: ValueString, Text: a.Str.Str}, nil
	case a.Number != nil:
		if a.Number.Unit != "" {
			return &Static{Kind: ValueDuration, Text: a.Number.Num + a.Number.Unit}, nil
		}
		return &Static{Kind: ValueNumber, Text: a.Number.Num}, nil
	}
	switch a.Name {
	case "true", "false":
		return &Static{Kind: ValueBool, Text: a.Name}, nil
	case "nil":
		return nilLiteral{}, nil
	}
	if isFieldName(a.Name) {
		return &Attribute{Name: a.Name}, nil
	}
	return &Static{Kind: ValueIdent, Text: a.Name}, nil
}

// newBinary builds `lhs op rhs`; a comparison with nil is a NilTest, and
// nil compared with nil is false.
func newBinary(op string, lhs, rhs FieldExpr) (FieldExpr, error) {
	_, lhsNil := lhs.(nilLiteral)
	_, rhsNil := rhs.(nilLiteral)
	if !lhsNil && !rhsNil {
		return &BinaryOp{Op: op, LHS: lhs, RHS: rhs}, nil
	}
	if op != "=" && op != "!=" {
		return nil, errUnexpectedNil
	}
	switch {
	case lhsNil && rhsNil:
		return &Static{Kind: ValueBool, Text: "false"}, nil
	case lhsNil:
		return &NilTest{Expr: rhs, Exists: op == "!="}, nil
	}
	return &NilTest{Expr: lhs, Exists: op == "!="}, nil
}

// newUnary builds `op e`; a minus before a number or duration literal is
// part of the literal.
func newUnary(op string, e FieldExpr) (FieldExpr, error) {
	if _, ok := e.(nilLiteral); ok {
		return nil, errUnexpectedNil
	}
	if s, ok := e.(*Static); ok && op == "-" && (s.Kind == ValueNumber || s.Kind == ValueDuration) {
		if text, negative := strings.CutPrefix(s.Text, "-"); negative {
			return &Static{Kind: s.Kind, Text: text}, nil
		}
		return &Static{Kind: s.Kind, Text: "-" + s.Text}, nil
	}
	return &UnaryOp{Op: op, Expr: e}, nil
}
