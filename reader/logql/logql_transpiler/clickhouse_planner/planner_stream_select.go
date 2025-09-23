package clickhouse_planner

import (
	"fmt"
	"strings"
	"time"

	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/reader/plugins"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type StreamSelectPlanner struct {
	LabelNames []string
	Ops        []string
	Values     []string
	Offset     *time.Duration
}

func NewStreamSelectPlanner(labelNames, ops, values []string, offset *time.Duration) shared.SQLRequestPlanner {
	p := plugins.GetStreamSelectPlannerPlugin()
	if p != nil {
		return (*p)(labelNames, ops, values)
	}
	return &StreamSelectPlanner{
		LabelNames: labelNames,
		Ops:        ops,
		Values:     values,
		Offset:     offset,
	}
}

func (s *StreamSelectPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	from := ctx.From
	if s.Offset != nil {
		from = from.Add(*s.Offset)
	} else if ctx.Offset.Nanoseconds() != 0 {
		from = from.Add(ctx.Offset)
	}

	clauses := make([]sql.SQLCondition, len(s.LabelNames))
	for i, name := range s.LabelNames {
		var valClause sql.SQLCondition
		switch s.Ops[i] {
		case "=":
			valClause = sql.Eq(sql.NewRawObject("val"), sql.NewStringVal(s.Values[i]))
		case "!=":
			valClause = sql.Neq(sql.NewRawObject("val"), sql.NewStringVal(s.Values[i]))
		case "=~":
			valClause = sql.Eq(&SqlMatch{
				col: sql.NewRawObject("val"), pattern: s.Values[i]}, sql.NewIntVal(1))
		case "!~":
			valClause = sql.Eq(&SqlMatch{
				col: sql.NewRawObject("val"), pattern: s.Values[i]}, sql.NewIntVal(0))
		default:
			return nil, &shared.NotSupportedError{
				Msg: fmt.Sprintf("%s op not supported", s.Ops[i]),
			}
		}
		clauses[i] = sql.And(
			sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(name)),
			valClause)
	}

	fpRequest := sql.NewSelect().
		Select(sql.NewRawObject("fingerprint")).
		From(sql.NewRawObject(ctx.TimeSeriesGinTableName)).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(FormatFromDate(from))),
			GetTypes(ctx),
			sql.Or(clauses...)).
		GroupBy(sql.NewRawObject("fingerprint")).
		AndHaving(sql.Eq(&SqlBitSetAnd{clauses}, sql.NewIntVal((1<<len(clauses))-1)))
	return fpRequest, nil
}

type SqlBitSetAnd struct {
	clauses []sql.SQLCondition
}

func NewSqlBitSetAnd(clauses []sql.SQLCondition) *SqlBitSetAnd {
	return &SqlBitSetAnd{clauses: clauses}
}

func (s *SqlBitSetAnd) String(ctx *sql.Ctx, options ...int) (string, error) {
	strConditions := make([]string, len(s.clauses))
	for i, c := range s.clauses {
		var err error
		strConditions[i], err = c.String(ctx, options...)
		if err != nil {
			return "", err
		}
		strConditions[i] = fmt.Sprintf("bitShiftLeft(%s, %d)", strConditions[i], i)
	}
	return fmt.Sprintf("groupBitOr(%s)", strings.Join(strConditions, " + ")), nil
}
