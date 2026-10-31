package clickhouse_transpiler

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

type AttrConditionPlanner struct {
	Main  shared.SQLRequestPlanner
	Terms []*attrComparison
	Conds *condition

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

	err = a.createWhere()
	if err != nil {
		return nil, err
	}

	having, err := a.getCond(a.Conds)
	if err != nil {
		return nil, err
	}

	res := main
	if len(a.where) > 0 {
		res = res.AndWhere(sql.Or(a.where...))
	}
	if having != nil {
		res = res.AndHaving(having)
	}

	restrictOperand(ctx, res)

	a.isAliased = false

	return res, nil
}

// createWhere builds the terms. The rows read are those of the terms that
// read an attribute key, or every row when the condition can hold without any.
func (a *AttrConditionPlanner) createWhere() error {
	a.sqlConds, a.where = nil, nil
	for _, t := range a.Terms {
		sqlTerm, err := a.getTerm(t)
		if err != nil {
			return err
		}
		a.sqlConds = append(a.sqlConds, sqlTerm)
		if readsKey(t) {
			a.where = append(a.where, sqlTerm)
		}
	}
	if a.holdsWithoutKeys(a.Conds) {
		a.where = nil
	}
	return nil
}

// readsKey reports whether term t holds only on the rows of one attribute
// key; the other terms hold on every row of a span or none.
func readsKey(t *attrComparison) bool {
	switch t.Attr {
	case "duration", "span:duration", "true", "false":
		return false
	}
	return true
}

// holdsWithoutKeys reports whether c can hold for a span none of whose rows
// satisfy a term that reads a key.
func (a *AttrConditionPlanner) holdsWithoutKeys(c *condition) bool {
	if c.simpleIdx != -1 {
		return !readsKey(a.Terms[c.simpleIdx])
	}
	for _, sub := range c.complex {
		holds := a.holdsWithoutKeys(sub)
		if c.op == "&&" && !holds {
			return false
		}
		if c.op != "&&" && holds {
			return true
		}
	}
	return c.op == "&&"
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

func (a *AttrConditionPlanner) getTerm(t *attrComparison) (sql.SQLCondition, error) {
	key := t.Attr
	// Normalize span: colon-intrinsic aliases to their canonical names.
	if key == "span:duration" {
		key = "duration"
	} else if key == "span:name" {
		key = "name"
	}
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
		case "true":
			return sql.Eq(sql.NewIntVal(1), sql.NewIntVal(1)), nil
		case "false":
			return sql.Eq(sql.NewIntVal(0), sql.NewIntVal(1)), nil
		case "status", "kind":
			// status and kind are stored as attributes in attrs_gin
			// (key='status', val='error'/'ok'/'unset') or (key='kind', val='server'/etc.)
			if t.Value.Kind == traceql_ir.ValueIdent {
				var valCond sql.SQLCondition
				switch t.Op {
				case "=":
					valCond = sql.Eq(sql.NewRawObject("val"), sql.NewStringVal(t.Value.Text))
				case "!=":
					valCond = sql.Neq(sql.NewRawObject("val"), sql.NewStringVal(t.Value.Text))
				default:
					return nil, fmt.Errorf("unsupported operator %s for %s", t.Op, key)
				}
				return sql.And(
					sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(key)),
					valCond,
				), nil
			}
			// Quoted values fall through to getTermStr
		case "statusMessage":
			// statusMessage falls through to getTermStr with unchanged key
		case "rootServiceName", "rootName":
			// Root-level intrinsics read the span's own service and name.
			if key == "rootServiceName" {
				key = "service.name"
			} else {
				key = "name"
			}
		default:
			return nil, fmt.Errorf("unsupported attribute %s", key)
		}
	}

	switch t.Value.Kind {
	case traceql_ir.ValueString:
		return a.getTermStr(t, key)
	case traceql_ir.ValueNumber:
		return a.getTermNum(t, key)
	case traceql_ir.ValueIdent:
		// Unquoted enum values like: status = error, kind != server, span.foo = bar
		// Respect the operator just like quoted strings.
		var valCond sql.SQLCondition
		switch t.Op {
		case "=", "":
			valCond = sql.Eq(sql.NewRawObject("val"), sql.NewStringVal(t.Value.Text))
		case "!=":
			valCond = sql.Neq(sql.NewRawObject("val"), sql.NewStringVal(t.Value.Text))
		default:
			return nil, fmt.Errorf("unsupported operator %s for unquoted value", t.Op)
		}
		return sql.And(
			sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(key)),
			valCond,
		), nil
	}
	return nil, fmt.Errorf("unsupported statement `%s`", t.String())
}

func (a *AttrConditionPlanner) getTermNum(t *attrComparison, key string) (sql.SQLCondition, error) {
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

	fVal, err := strconv.ParseFloat(t.Value.Text, 64)
	if err != nil {
		return nil, err
	}
	return sql.And(
		sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(key)),
		sql.Eq(sql.NewRawObject("isNotNull(toFloat64OrNull(val))"), sql.NewIntVal(1)),
		fn(sql.NewRawObject("toFloat64OrZero(val)"), sql.NewFloatVal(fVal)),
	), nil
}

func (a *AttrConditionPlanner) getTermStr(t *attrComparison, key string) (sql.SQLCondition, error) {
	switch t.Op {
	case "=":
		strVal, err := t.Value.Unquote()
		if err != nil {
			return nil, err
		}
		return sql.And(
			sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(key)),
			sql.Eq(sql.NewRawObject("val"), sql.NewStringVal(strVal)),
		), nil
	case "!=":
		strVal, err := t.Value.Unquote()
		if err != nil {
			return nil, err
		}
		return sql.And(
			sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(key)),
			sql.Neq(sql.NewRawObject("val"), sql.NewStringVal(strVal)),
		), nil
	case "=~":
		strVal, err := t.Value.Unquote()
		if err != nil {
			return nil, err
		}
		return sql.And(
			sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(key)),
			sql.Eq(&matchRe{sql.NewRawObject("val"), strVal}, sql.NewIntVal(1)),
		), nil
	case "!~":
		strVal, err := t.Value.Unquote()
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

func (a *AttrConditionPlanner) getTermDuration(t *attrComparison) (sql.SQLCondition, error) {
	if t.Value.Kind != traceql_ir.ValueDuration {
		return nil, fmt.Errorf(" is not a time duration value (%s)", t.String())
	}
	fVal, err := time.ParseDuration(t.Value.Text)
	if err != nil {
		return nil, err
	}

	fn, err := getComparisonFn(t.Op)
	if err != nil {
		return nil, err
	}

	return fn(sql.NewRawObject("traces_idx.duration"), sql.NewIntVal(fVal.Nanoseconds())), nil
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
