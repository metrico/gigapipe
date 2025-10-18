package clickhouse_planner

import (
	"fmt"
	"github.com/metrico/qryn/writer/config"
	"strings"
	"time"

	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type StreamSelectPlanner struct {
	NoStreamSelect bool
	LabelNames     []string
	Ops            []string
	Values         []string
	Offset         *time.Duration
}

func (s *StreamSelectPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	if s.NoStreamSelect {
		return nil, nil
	}
	from := ctx.From
	if s.Offset != nil {
		from = from.Add(*s.Offset)
	} else if ctx.Offset.Nanoseconds() != 0 {
		from = from.Add(ctx.Offset)
	}
	var emptyLabels []string
	for i := len(s.LabelNames) - 1; i >= 0; i-- {
		if config.Cloki.Setting.ClokiReader.OmitEmptyValues {
			break
		}
		if (s.Ops[i] == "=" || s.Ops[i] == "=~") && s.Values[i] == "" {
			emptyLabels = append(emptyLabels, s.LabelNames[i])
			s.LabelNames = append(s.LabelNames[:i], s.LabelNames[i+1:]...)
			s.Ops = append(s.Ops[:i], s.Ops[i+1:]...)
			s.Values = append(s.Values[:i], s.Values[i+1:]...)
		}
		if s.Ops[i] == "=~" && s.Values[i] == ".*" {
			s.LabelNames = append(s.LabelNames[:i], s.LabelNames[i+1:]...)
			s.Ops = append(s.Ops[:i], s.Ops[i+1:]...)
			s.Values = append(s.Values[:i], s.Values[i+1:]...)
		}
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
	return s.processEmptyLabels(ctx, fpRequest, emptyLabels)
}

func (s *StreamSelectPlanner) processEmptyLabels(ctx *shared.PlannerContext,
	req sql.ISelect, emptyLabels []string) (sql.ISelect, error) {
	if len(emptyLabels) == 0 {
		return req, nil
	}
	var withFpPreRequest *sql.With
	if len(s.LabelNames) > 0 {
		withFpPreRequest = sql.NewWith(req, "fp_pre_req")
	}
	var emptyClauses []sql.SQLCondition
	for _, label := range emptyLabels {
		_l := label
		c := sql.Eq(sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
			strL, err := sql.NewStringVal(_l).String(ctx, options...)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("simpleJSONHas(labels, %s)", strL), nil
		}), sql.NewIntVal(0))
		emptyClauses = append(emptyClauses, c)
	}
	res := sql.NewSelect().
		Select(sql.NewSimpleCol("fingerprint", "fingerprint")).
		From(sql.NewRawObject(ctx.TimeSeriesTableName)).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(FormatFromDate(ctx.From))),
			sql.And(emptyClauses...))
	if len(s.LabelNames) > 0 {
		res = res.
			With(withFpPreRequest).
			AndWhere(sql.NewIn(sql.NewRawObject("fingerprint"), sql.NewWithRef(withFpPreRequest)))
	}
	return res, nil
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
