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

// fieldPivot reads one row per span starting in range: trace_id, span_id,
// timestamp_ns, duration and one column per attribute key, NULL when the span
// lacks it. With no keys it reads the traces table.
type fieldPivot struct {
	keys []string
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
	return &fieldSQL{attr: p.column, duration: "duration"}
}

// spans plans the pivot over the spans that start in range.
func (p *fieldPivot) spans(ctx *shared.PlannerContext) sql.ISelect {
	if len(p.keys) == 0 {
		tsFrom, tsTo := timerange.SpanStart("traces.timestamp_ns", ctx.From.UnixNano(), ctx.To.UnixNano())
		return sql.NewSelect().
			Select(
				sql.NewSimpleCol("traces.trace_id", "trace_id"),
				sql.NewSimpleCol("traces.span_id", "span_id"),
				sql.NewSimpleCol("any(traces.timestamp_ns)", "timestamp_ns"),
				sql.NewSimpleCol("any(traces.duration_ns)", "duration")).
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

// attrsWhere selects the attribute rows of the pivot's keys in range.
func (p *fieldPivot) attrsWhere(ctx *shared.PlannerContext) []sql.SQLCondition {
	keys := make([]string, len(p.keys))
	for i, k := range p.keys {
		keys[i] = sqlString(k)
	}
	dateFrom, dateTo := timerange.SpanDate("traces_idx.date", ctx.From, ctx.To)
	tsFrom, tsTo := timerange.SpanStart("traces_idx.timestamp_ns", ctx.From.UnixNano(), ctx.To.UnixNano())
	return []sql.SQLCondition{
		dateFrom, dateTo,
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
	keys, err := fieldKeys(filter)
	if err != nil {
		return nil, err
	}
	if len(keys) > 0 && !falseWhenAbsent(filter) && !containsKey(keys, anchorKey) {
		keys = append(keys, anchorKey)
	}
	return &fieldPivot{keys: keys}, nil
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
	withSpans := sql.NewWith(spans, f.Prefix+"_pivot")
	return sql.NewSelect().
		With(withSpans).
		Select(append(spanRowCols(), sql.NewSimpleCol("duration", "duration"))...).
		From(sql.NewWithRef(withSpans)).
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
		return err == nil && !ref.duration
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
