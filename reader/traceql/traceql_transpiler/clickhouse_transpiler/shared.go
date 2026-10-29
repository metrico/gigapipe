package clickhouse_transpiler

import (
	"fmt"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

func getComparisonFn(op string) (func(left sql.SQLObject, right sql.SQLObject) *sql.LogicalOp, error) {
	switch op {
	case "=":
		return sql.Eq, nil
	case ">":
		return sql.Gt, nil
	case "<":
		return sql.Lt, nil
	case ">=":
		return sql.Ge, nil
	case "<=":
		return sql.Le, nil
	case "!=":
		return sql.Neq, nil
	}
	return nil, &shared.NotSupportedError{Msg: "not supported operator: " + op}
}

// inSubquery is `left IN (right)`, or GLOBAL IN on a cluster, where right
// reads Distributed tables.
func inSubquery(ctx *shared.PlannerContext, left sql.SQLObject, right sql.SQLObject) sql.SQLCondition {
	if ctx.IsCluster {
		return &globalIn{left: left, right: right}
	}
	return sql.NewIn(left, right)
}

// inFunc is `left IN (right)` in some form.
type inFunc func(left, right sql.SQLObject) sql.SQLCondition

// searchIn is inSubquery for ctx.
func searchIn(ctx *shared.PlannerContext) inFunc {
	return func(left, right sql.SQLObject) sql.SQLCondition { return inSubquery(ctx, left, right) }
}

// plainIn is `left IN (right)`, where right reads no Distributed table.
func plainIn(left, right sql.SQLObject) sql.SQLCondition { return sql.NewIn(left, right) }

type globalIn struct {
	left  sql.SQLObject
	right sql.SQLObject
}

func (g *globalIn) GetFunction() string { return "GLOBAL IN" }

func (g *globalIn) GetEntity() []sql.SQLObject { return []sql.SQLObject{g.left, g.right} }

func (g *globalIn) String(ctx *sql.Ctx, options ...int) (string, error) {
	l, err := g.left.String(ctx, options...)
	if err != nil {
		return "", err
	}
	r, err := g.right.String(ctx, options...)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s GLOBAL IN (%s)", l, r), nil
}

// tracesTable is the spans table search reads: Distributed on a cluster.
func tracesTable(ctx *shared.PlannerContext) string {
	if ctx.IsCluster {
		return ctx.TracesDistTable
	}
	return ctx.TracesTable
}

// attrsTable is the attribute index search reads: Distributed on a cluster.
func attrsTable(ctx *shared.PlannerContext) string {
	if ctx.IsCluster {
		return ctx.TracesAttrsDistTable
	}
	return ctx.TracesAttrsTable
}

// randomFilter keeps the traces of the current complexity portion and the
// traces already returned, or is nil outside portioned evaluation.
func randomFilter(ctx *shared.PlannerContext) sql.SQLCondition {
	if ctx.RandomFilter.Max == 0 {
		return nil
	}
	portion := sql.Eq(
		sql.NewRawObject(fmt.Sprintf("cityHash64(trace_id) %% %d", ctx.RandomFilter.Max)),
		sql.NewIntVal(int64(ctx.RandomFilter.I)))
	if len(ctx.CachedTraceIds) == 0 {
		return portion
	}
	cached := make([]sql.SQLObject, len(ctx.CachedTraceIds))
	for i, tid := range ctx.CachedTraceIds {
		cached[i] = sql.NewRawObject(fmt.Sprintf("unhex('%s')", tid))
	}
	return sql.Or(portion, sql.NewIn(sql.NewRawObject("trace_id"), cached...))
}

// restrictOperand keeps the rows of sel, an operand read, in the current
// complexity portion, among the spans of ctx.TracesOperandSpans and in the
// sample of ctx.TracesSample.
func restrictOperand(ctx *shared.PlannerContext, sel sql.ISelect) {
	for _, cond := range []sql.SQLCondition{randomFilter(ctx), narrowFilter(ctx), ctx.TracesSample} {
		if cond != nil {
			sel.AndWhere(cond)
		}
	}
}

// narrowFilter keeps the rows of the spans in ctx.TracesOperandSpans, or is
// nil when every span is a candidate: a semi-join on the trace, then on the span.
func narrowFilter(ctx *shared.PlannerContext) sql.SQLCondition {
	if ctx.TracesOperandSpans == nil {
		return nil
	}
	traces := sql.NewSelect().Select(sql.NewRawObject("trace_id")).From(sql.NewWithRef(ctx.TracesOperandSpans))
	spans := sql.NewSelect().
		Select(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id")).
		From(sql.NewWithRef(ctx.TracesOperandSpans))
	return sql.And(
		inSubquery(ctx, sql.NewRawObject("trace_id"), traces),
		inSubquery(ctx, sql.NewRawObject("(trace_id, span_id)"), spans))
}

// rawCond is a condition written as SQL.
type rawCond string

func (r rawCond) GetFunction() string { return "raw" }

func (r rawCond) GetEntity() []sql.SQLObject { return nil }

func (r rawCond) String(*sql.Ctx, ...int) (string, error) { return "(" + string(r) + ")", nil }
