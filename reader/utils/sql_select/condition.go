package sql_select

import (
	"fmt"
	"strings"
)

type LogicalOp struct {
	fn      string
	clauses []SQLObject
}

func (op *LogicalOp) GetFunction() string {
	return op.fn
}

func (op *LogicalOp) GetEntity() []SQLObject {
	return op.clauses
}

func (op *LogicalOp) AppendEntity(clauses ...SQLCondition) {
	for _, v := range clauses {
		op.clauses = append(op.clauses, v)
	}
}

func (op *LogicalOp) String(ctx *Ctx, options ...int) (string, error) {
	strClauses := make([]string, len(op.clauses))
	for i, c := range op.clauses {
		s, err := c.String(ctx, options...)
		if err != nil {
			return "", err
		}
		strClauses[i] = "(" + s + ")"
	}
	return strings.Join(strClauses, " "+op.fn+" "), nil
}

func NewGenericLogicalOp(fn string, clauses ...SQLCondition) *LogicalOp {
	_clauses := make([]SQLObject, len(clauses))
	for i, c := range clauses {
		_clauses[i] = c
	}
	return &LogicalOp{
		fn:      fn,
		clauses: _clauses,
	}
}

func And(clauses ...SQLCondition) *LogicalOp {
	return NewGenericLogicalOp("and", clauses...)
}

func Or(clauses ...SQLCondition) *LogicalOp {
	return NewGenericLogicalOp("or", clauses...)
}

func BinaryLogicalOp(fn string, left SQLObject, right SQLObject) *LogicalOp {
	return &LogicalOp{
		fn:      fn,
		clauses: []SQLObject{left, right},
	}
}

func Eq(left SQLObject, right SQLObject) *LogicalOp {
	return BinaryLogicalOp("==", left, right)
}

func Neq(left SQLObject, right SQLObject) *LogicalOp {
	return BinaryLogicalOp("!=", left, right)
}

func Lt(left SQLObject, right SQLObject) *LogicalOp {
	return BinaryLogicalOp("<", left, right)
}

func Le(left SQLObject, right SQLObject) *LogicalOp {
	return BinaryLogicalOp("<=", left, right)
}

func Gt(left SQLObject, right SQLObject) *LogicalOp {
	return BinaryLogicalOp(">", left, right)
}

func Ge(left SQLObject, right SQLObject) *LogicalOp {
	return BinaryLogicalOp(">=", left, right)
}

type CNot struct {
	expr SQLObject
}

func (n *CNot) GetFunction() string {
	return "NOT"
}

func (n *CNot) GetEntity() []SQLObject {
	return []SQLObject{n.expr}
}

func (n *CNot) String(ctx *Ctx, options ...int) (string, error) {
	str, err := n.expr.String(ctx, options...)
	return fmt.Sprintf("!(%s)", str), err
}

func Not(expr SQLObject) SQLCondition {
	return &CNot{expr: expr}
}

type CNotNull struct {
	expr SQLObject
}

func (c *CNotNull) GetFunction() string {
	return "IS NOT NULL"
}

func (c *CNotNull) GetEntity() []SQLObject {
	return []SQLObject{c.expr}
}

func (c *CNotNull) String(ctx *Ctx, options ...int) (string, error) {
	str, err := c.expr.String(ctx, options...)
	return fmt.Sprintf("%s IS NOT NULL", str), err
}

func NotNull(obj SQLObject) SQLCondition {
	return &CNotNull{expr: obj}
}
