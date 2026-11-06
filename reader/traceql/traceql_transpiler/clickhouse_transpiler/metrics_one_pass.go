package clickhouse_transpiler

import (
	"slices"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// onePassRead is how a metrics read finds a query's spans without an ids
// statement: the span rows of Result when set, else those Filter holds for over
// the read, else every span. Sample is the query's hash sample.
type onePassRead struct {
	Result shared.SQLRequestPlanner
	Filter traceql_ir.FieldExpr
	Sample sql.SQLCondition
}

// onePassOf is the read of pipeline, whose span rows are result. Select and
// coalesce stages keep the spans they get, so they don't decide the read.
func onePassOf(pipeline *traceql_ir.Pipeline, result shared.SQLRequestPlanner, sample sql.SQLCondition) *onePassRead {
	filters := &traceql_ir.Pipeline{}
	for _, e := range pipeline.Elements {
		switch e.(type) {
		case *traceql_ir.Select, *traceql_ir.Coalesce:
		default:
			filters.Elements = append(filters.Elements, e)
		}
	}
	if len(filters.Elements) == 0 {
		return &onePassRead{Sample: sample}
	}
	selector, ok := loneSelector(filters)
	if !ok {
		return &onePassRead{Result: result, Sample: sample}
	}
	if selector.Filter == nil {
		return &onePassRead{Sample: sample}
	}
	// A keyed pivot folds only to take its root flag from the read.
	if !(&simpleExpressionPlanner{selector: selector}).analyze() {
		pivot, err := newFieldPivotFor(selector.Filter)
		if err == nil && (len(pivot.keys) == 0 || pivot.root) {
			return &onePassRead{Filter: selector.Filter, Sample: sample}
		}
	}
	return &onePassRead{Result: result, Sample: sample}
}

// onePassOnEachShard is m's one-pass read, on each shard of ctx's cluster.
func (m *metricsSpansPlanner) onePassOnEachShard(ctx *shared.PlannerContext) (sql.ISelect, error) {
	if !ctx.IsCluster {
		return m.onePass(ctx)
	}
	shard, err := m.onePass(localContext(ctx))
	if err != nil {
		return nil, err
	}
	cols := []string{"trace_id", "span_id", "timestamp_ns", "duration_ns", "name", "service_name"}
	for _, col := range m.NestedSet {
		cols = append(cols, col.metrics())
	}
	res, err := onEachShard(ctx, shard, cols...)
	if err != nil {
		return nil, err
	}
	return clusterSettings(ctx, res), nil
}

// onePass reads the spans of m.OnePass over ctx's own tables. The read's filter
// takes attributes, if it has any, from a pivot that also bounds the read.
func (m *metricsSpansPlanner) onePass(ctx *shared.PlannerContext) (sql.ISelect, error) {
	ctx = sampled(ctx, m.OnePass.Sample)
	spans := metricsTracesRead(ctx)
	pairsIn := func(with *sql.With) sql.SQLCondition {
		return plainIn(sql.NewRawObject("(traces.trace_id, traces.span_id)"), sql.NewSelect().
			Select(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id")).From(sql.NewWithRef(with)))
	}
	var withs []*sql.With
	var pivot *fieldPivot
	var withAttrs *sql.With
	cols := m.NestedSet
	switch {
	case m.OnePass.Result != nil:
		main, err := m.OnePass.Result.Process(ctx)
		if err != nil {
			return nil, err
		}
		result := sql.NewWith(main, m.Prefix+"result_spans")
		withs = append(withs, result)
		spans.AndWhere(pairsIn(result))
	case m.OnePass.Filter == nil:
		restrictOperand(ctx, spans)
	default:
		var err error
		if pivot, err = newFieldPivotFor(m.OnePass.Filter); err != nil {
			return nil, err
		}
		if pivot.root {
			spans.Select(append(spans.GetSelect(), sql.NewSimpleCol(rootColumn, "_root"))...)
		}
		cols = slices.Clone(cols)
		for _, col := range pivot.nestedSet {
			if !slices.Contains(cols, col) {
				cols = append(cols, col)
			}
		}
		if len(pivot.keys) == 0 {
			restrictOperand(ctx, spans)
			break
		}
		attrs := pivot.spans(ctx)
		restrictOperand(ctx, attrs)
		withAttrs = sql.NewWith(attrs, m.Prefix+"metrics_attrs")
		withs = append(withs, withAttrs)
		spans.AndWhere(pairsIn(withAttrs))
	}
	if len(cols) == 0 && pivot == nil {
		return spans.With(withs...), nil
	}
	withSpans := sql.NewWith(spans, m.Prefix+"metrics_traces")
	s := withSpans.GetAlias()
	sel := columnsOf(s, "trace_id", "span_id", "timestamp_ns", "duration_ns", "name", "service_name")
	res := sql.NewSelect().From(sql.NewWithRef(withSpans))
	withs = append(withs, withSpans)
	if withAttrs != nil {
		res.AddJoin(sql.NewJoin("inner", sql.NewWithRef(withAttrs), sameSpan(s+".span_id", withAttrs.GetAlias()+".span_id")))
	}
	if len(cols) > 0 {
		ns := joinNestedSet(ctx, cols, plainIn, traceIdsOf(withSpans), nil, s, m.Prefix+"metrics_nested_set", nestedSetCol.metrics)
		sel = append(sel, ns.cols...)
		withs = append(withs, ns.with)
		res.AddJoin(ns.join)
	}
	if pivot != nil {
		fields := &fieldSQL{attr: pivot.column, duration: s + ".duration_ns", nestedSet: nestedSetCol.metrics, root: s + "._root"}
		if withAttrs != nil {
			fields.duration = withAttrs.GetAlias() + ".duration"
		}
		cond, err := fields.filter(m.OnePass.Filter)
		if err != nil {
			return nil, err
		}
		res.AndWhere(rawCond(cond))
	}
	return res.With(withs...).Select(sel...), nil
}
