package clickhouse_transpiler

import (
	"fmt"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// groupByStage splits each spanset by the value of an expression per span:
// it appends the value to every span's group key and, as the attribute named
// by the stage, to its spanset attributes.
type groupByStage struct {
	by *traceql_ir.GroupBy
	// pivot reads the expression's attribute keys; it holds every span
	// unless nilWhenAbsent.
	pivot *fieldPivot
}

func newGroupByStage(g *traceql_ir.GroupBy) (*groupByStage, error) {
	if err := traceql_ir.CheckGroupBy(g); err != nil {
		return nil, err
	}
	pivot, err := pivotOf(g.Expr)
	if err != nil {
		return nil, err
	}
	if len(pivot.keys) > 0 && !nilWhenAbsent(g.Expr) && !containsKey(pivot.keys, anchorKey) {
		pivot.keys = append(pivot.keys, anchorKey)
	}
	return &groupByStage{by: g, pivot: pivot}, nil
}

func (s *groupByStage) process(ctx *shared.PlannerContext, alias string, prev, _ *sql.With) (sql.ISelect, error) {
	pivot := s.pivot
	key, err := groupKey(pivot.sql(), s.by.Expr)
	if err != nil {
		return nil, err
	}
	spans := pivot.spans(ctx)
	table := "traces_idx"
	if len(pivot.keys) == 0 {
		table = "traces"
	}
	prevTraces := sql.NewSelect().Select(sql.NewRawObject("trace_id")).From(sql.NewWithRef(prev))
	prevSpans := sql.NewSelect().Select(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id")).From(sql.NewWithRef(prev))
	spans.AndWhere(
		inSubquery(ctx, sql.NewRawObject(table+".trace_id"), prevTraces),
		inSubquery(ctx, sql.NewRawObject(fmt.Sprintf("(%s.trace_id, %s.span_id)", table, table)), prevSpans))
	withs, withPivot := pivot.read(ctx, spans, alias+"_pivot")
	withKeys := sql.NewWith(sql.NewSelect().
		Select(
			sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("span_id", "span_id"),
			sql.NewSimpleCol(key, "key")).
		From(sql.NewWithRef(withPivot)), alias+"_keys")

	// A span missing from the pivot joins the default key, whose type is empty.
	p, k := prev.GetAlias(), withKeys.GetAlias()
	keyed := sql.NewSelect().
		Select(append(stageRowCols(p, p+".attrs", p+".grp"),
			sql.NewSimpleCol(fmt.Sprintf("if(tupleElement(%s.key, 4) = '', %s, %s.key)", k, nilGroupKey, k), "key"))...).
		From(sql.NewWithRef(prev)).
		Join(sql.NewJoin("left", sql.NewWithRef(withKeys), sameSpan(p+".span_id", k+".span_id")))
	withKeyed := sql.NewWith(keyed, alias+"_keyed")
	attr := fmt.Sprintf("tuple(%s, tupleElement(key, 4), tupleElement(key, 2), tupleElement(key, 3))", sqlString(s.by.String()))
	return sql.NewSelect().
		With(append(withs, withKeys, withKeyed)...).
		Select(stageRowCols("", "arrayPushBack(attrs, "+attr+")", "arrayPushBack(grp, key)")...).
		From(sql.NewWithRef(withKeyed)), nil
}

// groupKeyTuple is a group key value: (rank, value, text, type). Keys order
// by rank, then value, then text: numbers, bools, strings, then nil.
func groupKeyTuple(rank int, value, text, typ string) string {
	return fmt.Sprintf("tuple(toUInt8(%d), toFloat64(%s), toString(%s), %s)", rank, value, text, typ)
}

var nilGroupKey = groupKeyTuple(3, "0", "''", sqlString(AttrNil))

// groupKey compiles the group key value of e per span. A number keeps its
// type, a duration is in nanoseconds; an attribute is its stored text, which
// orders as a number when it reads as a finite one.
func groupKey(c *fieldSQL, e traceql_ir.FieldExpr) (string, error) {
	orNil := func(v, key string) string {
		return fmt.Sprintf("if(isNull(%s), %s, %s)", v, nilGroupKey, key)
	}
	switch domainOf(e) {
	case domainNumber:
		n, err := c.number(e)
		if err != nil {
			return "", err
		}
		typ := fmt.Sprintf("if(%s, %s, %s)", n.isInt, sqlString(AttrInt), sqlString(AttrFloat))
		if traceql_ir.TypeOf(e) == traceql_ir.TypeDuration {
			typ = sqlString(AttrDuration)
		}
		return orNil(n.val, groupKeyTuple(0, "assumeNotNull("+n.val+")", "''", typ)), nil
	case domainBool:
		b, err := c.boolean(e)
		if err != nil {
			return "", err
		}
		return orNil(b, groupKeyTuple(1, "assumeNotNull("+b+")", "''", sqlString(AttrBool))), nil
	case domainString:
		t, err := c.text(e)
		if err != nil {
			return "", err
		}
		return orNil(t, groupKeyTuple(2, "0", "assumeNotNull("+t+")", sqlString(AttrString))), nil
	}
	t, err := c.text(e)
	if err != nil {
		return "", err
	}
	f := "toFloat64OrNull(" + t + ")"
	return fmt.Sprintf("multiIf(isNull(%s), %s, isFinite(ifNull(%s, nan)), %s, %s)", t, nilGroupKey, f,
		groupKeyTuple(0, "assumeNotNull("+f+")", "assumeNotNull("+t+")", sqlString(AttrString)),
		groupKeyTuple(2, "0", "assumeNotNull("+t+")", sqlString(AttrString))), nil
}

// coalesceStage merges the spansets of each trace into one without attributes.
type coalesceStage struct{}

func (coalesceStage) process(_ *shared.PlannerContext, _ string, prev, _ *sql.With) (sql.ISelect, error) {
	return sql.NewSelect().
		Select(stageRowCols("", emptyAttrs, emptyGroup)...).
		From(sql.NewWithRef(prev)), nil
}
