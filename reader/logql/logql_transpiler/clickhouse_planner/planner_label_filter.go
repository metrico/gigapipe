package clickhouse_planner

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/metrico/qryn/v4/reader/logql/logql_parser"
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
	"golang.org/x/exp/slices"
)

type LabelFilterPlanner struct {
	Expr           *logql_parser.LabelFilter
	Main           shared.SQLRequestPlanner
	MainReq        sql.ISelect
	LabelValGetter func(string) sql.SQLObject
}

func (s *LabelFilterPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main := s.MainReq
	if main == nil {
		var err error
		main, err = s.Main.Process(ctx)
		if err != nil {
			return nil, err
		}
	}

	cond, err := s.makeSqlCond(ctx, s.Expr)
	if err != nil {
		return nil, err
	}
	return main.AndWhere(cond), nil
}

func (s *LabelFilterPlanner) makeSqlCond(ctx *shared.PlannerContext,
	expr *logql_parser.LabelFilter) (sql.SQLCondition, error) {
	var (
		leftSide  sql.SQLCondition
		rightSide sql.SQLCondition
		err       error
	)
	if expr.Head.SimpleHead != nil {
		leftSide, err = s.makeSimpleSqlCond(ctx, expr.Head.SimpleHead)
	} else {
		leftSide, err = s.makeSqlCond(ctx, expr.Head.ComplexHead)
	}
	if err != nil {
		return nil, err
	}
	if expr.Tail == nil {
		return leftSide, nil
	}

	rightSide, err = s.makeSqlCond(ctx, expr.Tail)
	if err != nil {
		return nil, err
	}
	switch expr.Op {
	case "and":
		return sql.And(leftSide, rightSide), nil
	case "or":
		return sql.Or(leftSide, rightSide), nil
	}
	return nil, errors.New("illegal expression " + expr.String())
}

func (s *LabelFilterPlanner) makeSimpleSqlCond(ctx *shared.PlannerContext, expr *logql_parser.SimpleLabelFilter) (sql.SQLCondition, error) {
	isNumeric := slices.Contains([]string{"==", ">", ">=", "<", "<="}, expr.Fn) ||
		(expr.Fn == "!=" && expr.StrVal == nil)

	if isNumeric {
		return s.makeSimpleNumSqlCond(ctx, expr)
	}
	return s.makeSimpleStrSqlCond(ctx, expr)
}

func (s *LabelFilterPlanner) makeSimpleStrSqlCond(_ *shared.PlannerContext, expr *logql_parser.SimpleLabelFilter) (sql.SQLCondition, error) {
	var label sql.SQLObject = sql.NewRawObject(fmt.Sprintf("labels['%s']", expr.Label.Name))
	if s.LabelValGetter != nil {
		label = s.LabelValGetter(expr.Label.Name)
	}

	var sqlOp func(left sql.SQLObject, right sql.SQLObject) *sql.LogicalOp
	switch expr.Fn {
	case "=":
		sqlOp = sql.Eq
	case "=~":
		sqlOp = func(left sql.SQLObject, right sql.SQLObject) *sql.LogicalOp {
			return sql.Eq(&SqlMatch{col: left, patternObj: right}, sql.NewIntVal(1))
		}
	case "!~":
		sqlOp = func(left sql.SQLObject, right sql.SQLObject) *sql.LogicalOp {
			return sql.Eq(&SqlMatch{col: left, patternObj: right}, sql.NewIntVal(0))
		}
	case "!=":
		sqlOp = sql.Neq
	}

	if expr.StrVal == nil || sqlOp == nil {
		return nil, errors.New("illegal expression: " + expr.String())
	}

	val, err := expr.StrVal.Unquote()
	if err != nil {
		return nil, err
	}
	return sqlOp(label, sql.NewStringVal(val)), nil
}

func (s *LabelFilterPlanner) makeSimpleNumSqlCond(_ *shared.PlannerContext, expr *logql_parser.SimpleLabelFilter) (sql.SQLCondition, error) {
	var label sql.SQLObject = sql.NewRawObject(fmt.Sprintf("labels['%s']", expr.Label.Name))
	if s.LabelValGetter != nil {
		label = s.LabelValGetter(expr.Label.Name)
	}
	label = &toFloat64OrNull{label}

	var sqlOp func(left sql.SQLObject, right sql.SQLObject) *sql.LogicalOp

	switch expr.Fn {
	case "==":
		sqlOp = sql.Eq
	case "!=":
		sqlOp = sql.Neq
	case ">":
		sqlOp = sql.Gt
	case ">=":
		sqlOp = sql.Ge
	case "<":
		sqlOp = sql.Lt
	case "<=":
		sqlOp = sql.Le
	}

	if expr.NumVal == "" {
		return nil, errors.New("illegal expression: " + expr.String())
	}
	val, err := strconv.ParseFloat(expr.NumVal, 64)
	if err != nil {
		return nil, err
	}
	return sql.And(
		&notNull{label},
		sqlOp(label, sql.NewFloatVal(val))), nil
}

type notNull struct {
	main sql.SQLObject
}

func (t *notNull) GetFunction() string {
	return "IS NOT NULL"
}
func (t *notNull) GetEntity() []sql.SQLObject {
	return []sql.SQLObject{t.main}
}

func (t *notNull) String(ctx *sql.Ctx, opts ...int) (string, error) {
	str, err := t.main.String(ctx, opts...)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s IS NOT NULL", str), nil
}

type toFloat64OrNull struct {
	main sql.SQLObject
}

func (t *toFloat64OrNull) String(ctx *sql.Ctx, opts ...int) (string, error) {
	str, err := t.main.String(ctx, opts...)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("toFloat64OrNull(%s)", str), nil
}
