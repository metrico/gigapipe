package clickhouse_transpiler

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/metrico/qryn/reader/logql/transpiler/shared"
	traceql_parser "github.com/metrico/qryn/reader/traceql/parser"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type AttrConditionPlanner struct {
	Main           shared.SQLRequestPlanner
	Terms          []*traceql_parser.AttrSelector
	Conds          *condition
	AggregatedAttr string

	sqlConds  []sql.SQLCondition
	isAliased bool
	alias     string
	where     []sql.SQLCondition
}

func (a *AttrConditionPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := a.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	a.alias = "bsCond"

	err = a.maybeCreateWhere()
	if err != nil {
		return nil, err
	}

	having, err := a.getCond(a.Conds)
	if err != nil {
		return nil, err
	}

	err = a.aggregator(main)
	if err != nil {
		return nil, err
	}

	res := main.AndWhere(sql.Or(a.where...)).AndHaving(having)

	if ctx.RandomFilter.Max != 0 && len(ctx.CachedTraceIds) > 0 {
		rawCachedTraceIds := make([]sql.SQLObject, len(ctx.CachedTraceIds))
		for i, tid := range ctx.CachedTraceIds {
			rawCachedTraceIds[i] = sql.NewRawObject(fmt.Sprintf("unhex('%s')", tid))
		}
		res.AndWhere(sql.Or(
			sql.Eq(
				sql.NewRawObject(fmt.Sprintf("cityHash64(trace_id) %% %d", ctx.RandomFilter.Max)),
				sql.NewIntVal(int64(ctx.RandomFilter.I)),
			),
			sql.NewIn(sql.NewRawObject("trace_id"), rawCachedTraceIds...),
		))
	} else if ctx.RandomFilter.Max != 0 {
		res.AndWhere(sql.Eq(
			sql.NewRawObject(fmt.Sprintf("cityHash64(trace_id) %% %d", ctx.RandomFilter.Max)),
			sql.NewIntVal(int64(ctx.RandomFilter.I)),
		))
	}

	a.isAliased = false

	return res, nil
}

func (a *AttrConditionPlanner) maybeCreateWhere() error {
	if len(a.sqlConds) > 0 {
		return nil
	}
	for _, t := range a.Terms {
		sqlTerm, err := a.getTerm(t)
		if err != nil {
			return err
		}
		a.sqlConds = append(a.sqlConds, sqlTerm)

		if !strings.HasPrefix(t.Label, "span.") &&
			!strings.HasPrefix(t.Label, "resource.") &&
			!strings.HasPrefix(t.Label, ".") &&
			t.Label != "name" {
			continue
		}
		a.where = append(a.where, sqlTerm)
	}
	return nil
}

func (a *AttrConditionPlanner) aggregator(main sql.ISelect) error {
	if a.AggregatedAttr == "" {
		return nil
	}

	s := main.GetSelect()
	if a.AggregatedAttr == "duration" {
		s = append(s, sql.NewSimpleCol("toFloat64(duration)", "agg_val"))
		main.Select(s...)
		return nil
	}

	if strings.TrimPrefix(a.AggregatedAttr, "span.") != a.AggregatedAttr {
		a.AggregatedAttr = a.AggregatedAttr[5:]
	}
	if strings.TrimPrefix(a.AggregatedAttr, "resource.") != a.AggregatedAttr {
		a.AggregatedAttr = a.AggregatedAttr[9:]
	}
	if strings.TrimPrefix(a.AggregatedAttr, ".") != a.AggregatedAttr {
		a.AggregatedAttr = a.AggregatedAttr[1:]
	}
	s = append(s, sql.NewCol(&sqlAttrValue{a.AggregatedAttr}, "agg_val"))
	main.Select(s...)
	a.where = append(a.where, sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(a.AggregatedAttr)))
	return nil
}

func (a *AttrConditionPlanner) getCond(c *condition) (sql.SQLCondition, error) {
	if c.simpleIdx == -1 {
		subs := make([]sql.SQLCondition, len(c.complex))
		for i, s := range c.complex {
			cond, err := a.getCond(s)
			if err != nil {
				return nil, err
			}
			subs[i] = cond
		}
		switch c.op {
		case "&&":
			return sql.And(subs...), nil
		}
		return sql.Or(subs...), nil
	}
	var left sql.SQLObject
	if !a.isAliased {
		left = &groupBitOr{&bitSet{
			terms: a.sqlConds,
		}, a.alias}
		a.isAliased = true
	} else {
		left = sql.NewRawObject(a.alias)
	}
	return sql.Neq(&bitAnd{left, sql.NewIntVal(int64(1) << c.simpleIdx)}, sql.NewIntVal(0)), nil
}

func (a *AttrConditionPlanner) getTerm(t *traceql_parser.AttrSelector) (sql.SQLCondition, error) {
	key := t.Label
	if strings.HasPrefix(key, "span.") {
		key = key[5:]
	} else if strings.HasPrefix(key, "resource.") {
		key = key[9:]
	} else if strings.HasPrefix(key, ".") {
		key = key[1:]
	} else {
		switch key {
		case "duration":
			return a.getTermDuration(t)
		case "name":
			key = "name"
		default:
			return nil, fmt.Errorf("unsupported attribute %s", key)
		}
	}

	if t.Val.StrVal != nil {
		return a.getTermStr(t, key)
	} else if t.Val.FVal != "" {
		return a.getTermNum(t, key)
	}
	return nil, fmt.Errorf("unsupported statement `%s`", t.String())
}

func (a *AttrConditionPlanner) getTermNum(t *traceql_parser.AttrSelector, key string) (sql.SQLCondition, error) {
	var fn func(left sql.SQLObject, right sql.SQLObject) *sql.LogicalOp
	switch t.Op {
	case "=":
		fn = sql.Eq
	case "!=":
		fn = sql.Neq
	case ">":
		fn = sql.Gt
	case "<":
		fn = sql.Lt
	case ">=":
		fn = sql.Ge
	case "<=":
		fn = sql.Le
	default:
		return nil, &shared.NotSupportedError{Msg: "not supported operator: " + t.Op}
	}

	if t.Val.FVal == "" {
		return nil, fmt.Errorf("%s is not a number value (%s)", t.Val.FVal, t.String())
	}
	fVal, err := strconv.ParseFloat(t.Val.FVal, 64)
	if err != nil {
		return nil, err
	}
	return sql.And(
		sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(key)),
		sql.Eq(sql.NewRawObject("isNotNull(toFloat64OrNull(val))"), sql.NewIntVal(1)),
		fn(sql.NewRawObject("toFloat64OrZero(val)"), sql.NewFloatVal(fVal)),
	), nil
}

func (a *AttrConditionPlanner) getTermStr(t *traceql_parser.AttrSelector, key string) (sql.SQLCondition, error) {
	switch t.Op {
	case "=":
		strVal, err := a.getString(t)
		if err != nil {
			return nil, err
		}
		return sql.And(
			sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(key)),
			sql.Eq(sql.NewRawObject("val"), sql.NewStringVal(strVal)),
		), nil
	case "!=":
		strVal, err := a.getString(t)
		if err != nil {
			return nil, err
		}
		return sql.And(
			sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(key)),
			sql.Neq(sql.NewRawObject("val"), sql.NewStringVal(strVal)),
		), nil
	case "=~":
		strVal, err := a.getString(t)
		if err != nil {
			return nil, err
		}
		return sql.And(
			sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(key)),
			sql.Eq(&matchRe{sql.NewRawObject("val"), strVal}, sql.NewIntVal(1)),
		), nil
	case "!~":
		strVal, err := a.getString(t)
		if err != nil {
			return nil, err
		}
		return sql.And(
			sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(key)),
			sql.Eq(&matchRe{sql.NewRawObject("val"), strVal}, sql.NewIntVal(0)),
		), nil
	}
	return nil, &shared.NotSupportedError{Msg: "not supported operator: " + t.Op}
}

func (a *AttrConditionPlanner) getTermDuration(t *traceql_parser.AttrSelector) (sql.SQLCondition, error) {
	if t.Val.TimeVal == "" {
		return nil, fmt.Errorf("%s is not a time duration value (%s)", t.Val.TimeVal, t.String())
	}
	fVal, err := time.ParseDuration(t.Val.TimeVal)
	if err != nil {
		return nil, err
	}

	fn, err := getComparisonFn(t.Op)
	if err != nil {
		return nil, err
	}

	return fn(sql.NewRawObject("traces_idx.duration"), sql.NewIntVal(fVal.Nanoseconds())), nil
}

func (a *AttrConditionPlanner) getString(t *traceql_parser.AttrSelector) (string, error) {
	var (
		strVal string
		err    error
	)
	if t.Val.StrVal != nil {
		strVal, err = t.Val.StrVal.Unquote()
		if err != nil {
			return "", err
		}
	} else {
		strVal = t.Val.FVal
	}
	return strVal, nil
}

type bitSet struct {
	terms []sql.SQLCondition
}

func (b *bitSet) String(ctx *sql.Ctx, options ...int) (string, error) {
	strTerms := make([]string, len(b.terms))
	for i, term := range b.terms {
		strTerm, err := term.String(ctx, options...)
		if err != nil {
			return "", err
		}
		strTerms[i] = fmt.Sprintf("bitShiftLeft(toUInt64(%s),%d)", strTerm, i)
	}
	res := strings.Join(strTerms, "+")

	return res, nil
}

type bitAnd struct {
	left  sql.SQLObject
	right sql.SQLObject
}

func (b *bitAnd) String(ctx *sql.Ctx, options ...int) (string, error) {

	strLeft, err := b.left.String(ctx, options...)
	if err != nil {
		return "", err
	}
	strRight, err := b.right.String(ctx, options...)
	if err != nil {
		return "", err
	}
	res := fmt.Sprintf("bitAnd(%s,%s)", strLeft, strRight)

	return res, nil
}

type groupBitOr struct {
	left  sql.SQLObject
	alias string
}

func (b *groupBitOr) String(ctx *sql.Ctx, options ...int) (string, error) {

	strLeft, err := b.left.String(ctx, options...)
	if err != nil {
		return "", err
	}
	res := fmt.Sprintf("groupBitOr(%s)", strLeft)
	if b.alias != "" {
		res = fmt.Sprintf("%s as %s", res, b.alias)
	}
	return res, nil
}

type matchRe struct {
	field sql.SQLObject
	re    string
}

func (m matchRe) String(ctx *sql.Ctx, options ...int) (string, error) {
	field, err := m.field.String(ctx, options...)
	if err != nil {
		return "", err
	}
	strRe, err := sql.NewStringVal(m.re).String(ctx, options...)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("match(%s,%s)", field, strRe), nil
}

type sqlAttrValue struct {
	attr string
}

func (s *sqlAttrValue) String(ctx *sql.Ctx, options ...int) (string, error) {
	attr, err := sql.NewStringVal(s.attr).String(ctx, options...)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("anyIf(toFloat64OrNull(val), key == %s)", attr), nil
}
