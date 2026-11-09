package clickhouse_transpiler

import (
	"fmt"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// emptyAttrs is the spanset attributes column of a stage that sets none:
// (name, type, value, text) per aggregate or group key. type is one of the
// Attr* types; a number or a bool is in value, a duration in nanoseconds,
// and a string in text.
const emptyAttrs = "CAST([], 'Array(Tuple(String, String, Float64, String))')"

// emptyGroup is the group key column of an ungrouped stage. A group key is
// one (rank, value, text, type) tuple per by() stage; see groupKey.
const emptyGroup = "CAST([], 'Array(Tuple(UInt8, Float64, String, String))')"

// planResult plans the result span rows of the query's pipeline. With
// prepick, a lone `{}` reads only the ctx.Limit traces with the newest spans.
func (p *planner) planResult(prepick bool) (shared.SQLRequestPlanner, error) {
	if prepick && AllSpans(p.query) {
		return &pipelinePlanner{
			Stages: []pipelineStage{&spansetStage{Expr: NewAttrlessConditionPlanner()}},
			Prefix: p.getPrefix(),
		}, nil
	}
	return p.planQuery()
}

// readsEverySpan reports whether a pipeline starts with a scalar filter, a
// by() or a select(), which read every span in range.
func readsEverySpan(pipeline *traceql_ir.Pipeline) bool {
	switch pipeline.Elements[0].(type) {
	case *traceql_ir.ScalarFilter, *traceql_ir.GroupBy, *traceql_ir.Select:
		return true
	}
	return false
}

// planQuery plans the query's pipeline into its result span rows, rejecting
// spanset operators over grouped spansets.
func (p *planner) planQuery() (shared.SQLRequestPlanner, error) {
	if err := traceql_ir.CheckGrouping(&p.query.Pipeline); err != nil {
		return nil, err
	}
	return p.planPipeline(&p.query.Pipeline)
}

// planPipeline plans a pipeline into its result span rows.
func (p *planner) planPipeline(pipeline *traceql_ir.Pipeline) (shared.SQLRequestPlanner, error) {
	var stages []pipelineStage
	if readsEverySpan(pipeline) {
		stages = append(stages, &spansetStage{Expr: &AllSpansPlanner{}})
	}
	for _, element := range pipeline.Elements {
		switch e := element.(type) {
		case *traceql_ir.ScalarFilter:
			stage, err := newScalarFilterStage(e)
			if err != nil {
				return nil, err
			}
			stages = append(stages, stage)
		case *traceql_ir.GroupBy:
			stage, err := newGroupByStage(e)
			if err != nil {
				return nil, err
			}
			stages = append(stages, stage)
		case *traceql_ir.Coalesce:
			stages = append(stages, coalesceStage{})
		case *traceql_ir.Select:
			// The selected values are read for the listed spans of the result.
		case traceql_ir.SpansetExpr:
			expr, err := p.planSpansetExpr(e)
			if err != nil {
				return nil, err
			}
			stages = append(stages, &spansetStage{Expr: expr, Pipeline: isPipeline(e)})
		}
	}
	return &pipelinePlanner{Stages: stages, Prefix: p.getPrefix()}, nil
}

// isPipeline reports whether expr is a parenthesised pipeline.
func isPipeline(expr traceql_ir.SpansetExpr) bool {
	for {
		switch e := expr.(type) {
		case *traceql_ir.Group:
			expr = e.Expr
		case *traceql_ir.Pipeline:
			return true
		default:
			return false
		}
	}
}

// pipelineStage plans one stage over prev, the previous stage's rows, and
// base, the rows of the last spanset stage before it. base is nil for the
// first stage, and prev too unless the pipeline is nested.
type pipelineStage interface {
	process(ctx *shared.PlannerContext, alias string, prev, base *sql.With) (sql.ISelect, error)
}

// pipelinePlanner evaluates its stages in order, one CTE each, into span
// rows: trace_id, span_id, timestamp_ns, attrs, the spanset attributes, and
// grp, the group key. The rows of a trace with the same group key form one
// spanset. A nested pipeline reads ctx.TracesOperandSpans as the stage before
// its first.
type pipelinePlanner struct {
	Stages []pipelineStage
	Prefix string
}

func (p *pipelinePlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	prev := ctx.TracesOperandSpans
	var base *sql.With
	withs := make([]*sql.With, len(p.Stages))
	for i, stage := range p.Stages {
		alias := fmt.Sprintf("%sstage%d", p.Prefix, i)
		sel, err := stage.process(ctx, alias, prev, base)
		if err != nil {
			return nil, err
		}
		withs[i] = sql.NewWith(sel, alias)
		prev = withs[i]
		if _, ok := stage.(*spansetStage); ok {
			base = prev
		}
	}
	return sql.NewSelect().
		With(withs...).
		Select(stageRowCols("", "attrs", "grp")...).
		From(sql.NewWithRef(prev)), nil
}

// stageRowCols are the columns of a stage row: the span-row columns of the
// given table alias, attrs and grp.
func stageRowCols(alias, attrs, grp string) []sql.SQLObject {
	return append(spanRowColsOf(alias), sql.NewSimpleCol(attrs, "attrs"), sql.NewSimpleCol(grp, "grp"))
}

// spansetStage is the span rows of a spanset expression. After another stage, its
// selectors match only that stage's spans, whose attributes and group keys
// it keeps.
type spansetStage struct {
	Expr shared.SQLRequestPlanner
	// Pipeline is set when Expr is a nested pipeline, which reads the
	// previous stage itself and whose rows carry the attributes and group keys.
	Pipeline bool
}

func (s *spansetStage) process(ctx *shared.PlannerContext, alias string, prev, _ *sql.With) (sql.ISelect, error) {
	exprCtx := *ctx
	if prev != nil {
		exprCtx.TracesOperandSpans = prev
	}
	main, err := s.Expr.Process(&exprCtx)
	if err != nil {
		return nil, err
	}
	withExpr := sql.NewWith(main, alias+"_expr")
	e := withExpr.GetAlias()
	attrs, grp := emptyAttrs, emptyGroup
	switch {
	case s.Pipeline:
		attrs, grp = e+".attrs", e+".grp"
	case prev != nil:
		attrs, grp = prev.GetAlias()+".attrs", prev.GetAlias()+".grp"
	}
	res := sql.NewSelect().
		With(withExpr).
		Select(stageRowCols(e, attrs, grp)...).
		From(sql.NewWithRef(withExpr))
	if prev != nil && !s.Pipeline {
		res.Join(sql.NewJoin("inner", sql.NewWithRef(prev), sameSpan(prev.GetAlias()+".span_id", e+".span_id")))
	}
	return res, nil
}
