package clickhouse_transpiler

import (
	"fmt"
	"strings"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/traceql/timerange"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// anchorKey is the attribute every span has; a pivot that must see spans
// lacking all the keys it reads reads this one too.
const anchorKey = "name"

// fieldPivot reads one row per span starting in range with the values its
// filter reads; an absent attribute is NULL.
type fieldPivot struct {
	keys      []string
	nestedSet []nestedSetCol
	root      bool
}

func (p *fieldPivot) column(key string) string {
	for i, k := range p.keys {
		if k == key {
			return fmt.Sprintf("_f%d", i)
		}
	}
	return "NULL"
}

func (p *fieldPivot) sql() *fieldSQL {
	return &fieldSQL{attr: p.column, duration: "duration", nestedSet: nestedSetCol.pivot, root: "_root"}
}

// spans plans the attribute rows of the pivot over the spans that start in
// range; read completes them.
func (p *fieldPivot) spans(ctx *shared.PlannerContext) sql.ISelect {
	if len(p.keys) == 0 {
		tsFrom, tsTo := timerange.SpanStart("traces.timestamp_ns", ctx.From.UnixNano(), ctx.To.UnixNano())
		cols := []sql.SQLObject{
			sql.NewSimpleCol("traces.trace_id", "trace_id"),
			sql.NewSimpleCol("traces.span_id", "span_id"),
			sql.NewSimpleCol("any(traces.timestamp_ns)", "timestamp_ns"),
			sql.NewSimpleCol("any(traces.duration_ns)", "duration"),
		}
		if p.root {
			cols = append(cols, sql.NewSimpleCol(rootColumn, "_root"))
		}
		return sql.NewSelect().
			Select(cols...).
			From(sql.NewSimpleCol(tracesTable(ctx), "traces")).
			AndWhere(tsFrom, tsTo).
			GroupBy(sql.NewRawObject("traces.trace_id"), sql.NewRawObject("traces.span_id"))
	}
	cols := []sql.SQLObject{
		sql.NewSimpleCol("traces_idx.trace_id", "trace_id"),
		sql.NewSimpleCol("traces_idx.span_id", "span_id"),
		sql.NewSimpleCol("any(traces_idx.timestamp_ns)", "timestamp_ns"),
		sql.NewSimpleCol("any(traces_idx.duration)", "duration"),
	}
	for _, k := range p.keys {
		cols = append(cols, sql.NewSimpleCol(
			fmt.Sprintf("maxIfOrNull(traces_idx.val, traces_idx.key = %s)", sqlString(k)), p.column(k)))
	}
	return sql.NewSelect().
		Select(cols...).
		From(sql.NewSimpleCol(attrsTable(ctx), "traces_idx")).
		AndWhere(p.attrsWhere(ctx)...).
		GroupBy(sql.NewRawObject("traces_idx.trace_id"), sql.NewRawObject("traces_idx.span_id"))
}

// rootColumn is 1 for a span any copy of which has no parent id.
const rootColumn = "max(traces.parent_id = '')"

// read returns the CTEs that read the pivot of spans, from spans(), and the
// last of them, named alias, which holds every pivot column.
func (p *fieldPivot) read(ctx *shared.PlannerContext, spans sql.ISelect, alias string) (withs []*sql.With, pivot *sql.With) {
	joinsRoot := p.root && len(p.keys) > 0
	if len(p.nestedSet) == 0 && !joinsRoot {
		pivot = sql.NewWith(spans, alias)
		return []*sql.With{pivot}, pivot
	}
	withAttrs := sql.NewWith(spans, alias+"_attrs")
	a := withAttrs.GetAlias()
	traceIds := traceIdsOf(withAttrs)
	names := []string{"trace_id", "span_id", "timestamp_ns", "duration"}
	for _, k := range p.keys {
		names = append(names, p.column(k))
	}
	cols := columnsOf(a, names...)
	withs = []*sql.With{withAttrs}
	var joins []*sql.Join
	if len(p.nestedSet) > 0 {
		ns := joinNestedSet(ctx, p.nestedSet, searchIn(ctx), traceIds, nil, a, alias+"_ns", nestedSetCol.pivot)
		cols = append(cols, ns.cols...)
		withs = append(withs, ns.with)
		joins = append(joins, ns.join)
	}
	switch {
	case joinsRoot:
		withRoot := sql.NewWith(sql.NewSelect().
			Select(
				sql.NewSimpleCol("traces.trace_id", "trace_id"),
				sql.NewSimpleCol("traces.span_id", "span_id"),
				sql.NewSimpleCol(rootColumn, "root")).
			From(sql.NewSimpleCol(tracesTable(ctx), "traces")).
			AndWhere(inSubquery(ctx, sql.NewRawObject("traces.trace_id"), traceIds)).
			GroupBy(sql.NewRawObject("traces.trace_id"), sql.NewRawObject("traces.span_id")), alias+"_root")
		r := withRoot.GetAlias()
		cols = append(cols, sql.NewSimpleCol(fmt.Sprintf("ifNull(%s.root, 0)", r), "_root"))
		withs = append(withs, withRoot)
		joins = append(joins, sql.NewJoin("left", sql.NewWithRef(withRoot), sameSpan(a+".span_id", r+".span_id")))
	case p.root:
		cols = append(cols, sql.NewSimpleCol(a+"._root", "_root"))
	}
	res := sql.NewSelect().Select(cols...).From(sql.NewWithRef(withAttrs))
	for _, j := range joins {
		res.AddJoin(j)
	}
	pivot = sql.NewWith(res, alias)
	return append(withs, pivot), pivot
}

// attrsWhere selects the attribute rows of the pivot's keys in range.
func (p *fieldPivot) attrsWhere(ctx *shared.PlannerContext) []sql.SQLCondition {
	keys := make([]string, len(p.keys))
	for i, k := range p.keys {
		keys[i] = sqlString(k)
	}
	tsFrom, tsTo := timerange.SpanStart("traces_idx.timestamp_ns", ctx.From.UnixNano(), ctx.To.UnixNano())
	return []sql.SQLCondition{
		sql.Ge(sql.NewRawObject("traces_idx.date"), sql.NewStringVal(ctx.From.Format("2006-01-02"))),
		sql.Le(sql.NewRawObject("traces_idx.date"), sql.NewStringVal(ctx.To.Format("2006-01-02"))),
		tsFrom, tsTo,
		rawCond("traces_idx.key IN (" + strings.Join(keys, ", ") + ")"),
	}
}

// FieldFilterPlanner is a selector whose filter is not a boolean combination
// of `attribute op literal`: the span rows of a pivot of the attributes the
// filter reads, kept where the filter holds.
type FieldFilterPlanner struct {
	Filter traceql_ir.FieldExpr
	Prefix string
}

// newFieldPivotFor plans the pivot filter reads. The pivot reads every span
// unless the filter is false for spans lacking all its keys.
func newFieldPivotFor(filter traceql_ir.FieldExpr) (*fieldPivot, error) {
	pivot, err := pivotOf(filter)
	if err != nil {
		return nil, err
	}
	if len(pivot.keys) > 0 && !falseWhenAbsent(filter) && !containsKey(pivot.keys, anchorKey) {
		pivot.keys = append(pivot.keys, anchorKey)
	}
	return pivot, nil
}

func (f *FieldFilterPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	pivot, err := newFieldPivotFor(f.Filter)
	if err != nil {
		return nil, err
	}
	cond, err := pivot.sql().filter(f.Filter)
	if err != nil {
		return nil, err
	}
	spans := pivot.spans(ctx)
	restrictOperand(ctx, spans)
	withs, withPivot := pivot.read(ctx, spans, f.Prefix+"_pivot")
	return sql.NewSelect().
		With(withs...).
		Select(append(spanRowCols(), sql.NewSimpleCol("duration", "duration"))...).
		From(sql.NewWithRef(withPivot)).
		AndWhere(rawCond(cond)), nil
}

// FieldFilterEvaluatorPlanner estimates FieldFilterPlanner as the rows its
// pivot reads.
type FieldFilterEvaluatorPlanner struct {
	Filter traceql_ir.FieldExpr
	Prefix string
}

func (f *FieldFilterEvaluatorPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	pivot, err := newFieldPivotFor(f.Filter)
	if err != nil {
		return nil, err
	}
	if len(pivot.keys) == 0 {
		return (&AllSpansEvaluatorPlanner{Prefix: f.Prefix}).Process(ctx)
	}
	return sql.NewSelect().
		Select(
			sql.NewCol(sql.NewStringVal(f.Prefix), "prefix"),
			sql.NewSimpleCol("count()", "_count")).
		From(sql.NewSimpleCol(attrsTable(ctx), "traces_idx")).
		AndWhere(pivot.attrsWhere(ctx)...), nil
}

func containsKey(keys []string, key string) bool {
	for _, k := range keys {
		if k == key {
			return true
		}
	}
	return false
}

// falseWhenAbsent reports whether filter e is false for every span that
// lacks all the attribute keys e reads.
func falseWhenAbsent(e traceql_ir.FieldExpr) bool {
	switch e := e.(type) {
	case *traceql_ir.Attribute:
		return nilWhenAbsent(e)
	case *traceql_ir.Static:
		return e.Kind == traceql_ir.ValueBool && e.Text == "false"
	case *traceql_ir.BinaryOp:
		switch e.Op {
		case "&&":
			return falseWhenAbsent(e.LHS) || falseWhenAbsent(e.RHS)
		case "||":
			return falseWhenAbsent(e.LHS) && falseWhenAbsent(e.RHS)
		}
		return nilWhenAbsent(e.LHS) || nilWhenAbsent(e.RHS)
	case *traceql_ir.UnaryOp:
		return nilWhenAbsent(e.Expr)
	case *traceql_ir.NilTest:
		return e.Exists && nilWhenAbsent(e.Expr)
	}
	return false
}

// nilWhenAbsent reports whether value e is nil for every span that lacks all
// the attribute keys e reads.
func nilWhenAbsent(e traceql_ir.FieldExpr) bool {
	switch e := e.(type) {
	case *traceql_ir.Attribute:
		ref, err := fieldRefOf(e.Name)
		return err == nil && !ref.duration && ref.nestedSet == ""
	case *traceql_ir.BinaryOp:
		if traceql_ir.IsArithmetic(e.Op) {
			return nilWhenAbsent(e.LHS) || nilWhenAbsent(e.RHS)
		}
	case *traceql_ir.UnaryOp:
		if e.Op == "-" {
			return nilWhenAbsent(e.Expr)
		}
	}
	return false
}
