package clickhouse_transpiler

import (
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/traceql/timerange"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// MetricsBuild builds a metrics query reading spans, a CTE with one row per
// span: trace_id, span_id, timestamp_ns, duration_ns, name, service_name and
// the MetricsNestedSetColumn of each nested-set field the function reads.
type MetricsBuild func(spans *sql.With) (sql.ISelect, error)

// CompareBuild builds compare() from two CTEs shaped like MetricsBuild's:
// baseline, the result spans over the baseline window, and selection, the
// spans of the selection filter over the query range, or nil for `{}`.
type CompareBuild func(baseline, selection *sql.With) (sql.ISelect, error)

// PlanMetrics plans a metrics query over the result spans of the query's
// spanset expression in the query range: the spans a search for it returns,
// uncapped.
func PlanMetrics(query *traceql_ir.Query, build MetricsBuild) (shared.SQLRequestPlanner, error) {
	p := &planner{query: query}
	spans, err := p.metricsSpans()
	if err != nil {
		return nil, err
	}
	spans.NestedSet = metricsNestedSet(query.Metrics)
	return p.structuralGate(&metricsPlanner{Spans: spans, Sample: sampleFilter(query), Build: build}), nil
}

// PlanCompare plans compare(): the query's result spans over
// [baselineFrom, baselineTo] are the baseline, and the spans of the selection
// filter over the query range are the selection.
func PlanCompare(query *traceql_ir.Query, selection traceql_ir.FieldExpr,
	baselineFrom, baselineTo time.Time, build CompareBuild) (shared.SQLRequestPlanner, error) {
	p := &planner{query: query}
	baseline, err := p.metricsSpans()
	if err != nil {
		return nil, err
	}
	res := &comparePlanner{Baseline: baseline, BaselineFrom: baselineFrom, BaselineTo: baselineTo,
		Sample: sampleFilter(query), Build: build}
	if selection != nil {
		expr, err := p.operand(&traceql_ir.Selector{Filter: selection}).planner()
		if err != nil {
			return nil, err
		}
		filter := &pipelinePlanner{
			Stages: []pipelineStage{&spansetStage{Expr: expr}},
			Prefix: p.getPrefix(),
		}
		res.Selection = &metricsSpansPlanner{Result: filter, Prefix: p.getPrefix()}
	}
	return p.structuralGate(res), nil
}

// sampleFilter is the hash sample of the query's sample hint, or nil without
// one: by trace when the query relates spans across a trace, else by span.
func sampleFilter(query *traceql_ir.Query) sql.SQLCondition {
	f, ok := query.SampleFraction()
	if !ok {
		return nil
	}
	col := "span_id"
	if query.NeedsWholeTrace() {
		col = "trace_id"
	}
	return rawCond(fmt.Sprintf("cityHash64(%s) < %d", col, sampleThreshold(f)))
}

// sampleThreshold is f·2^64, the hash bound below which a fraction f of
// uniform 64-bit hashes falls.
func sampleThreshold(f float64) uint64 {
	return uint64(math.Ldexp(f, 64))
}

// sampled returns ctx with sample set as its operand sample, or ctx itself
// without one.
func sampled(ctx *shared.PlannerContext, sample sql.SQLCondition) *shared.PlannerContext {
	if sample == nil {
		return ctx
	}
	res := *ctx
	res.TracesSample = sample
	return &res
}

// metricsSpans plans the metrics spans of the query's pipeline.
func (p *planner) metricsSpans() (*metricsSpansPlanner, error) {
	result, err := p.planQuery()
	if err != nil {
		return nil, err
	}
	return &metricsSpansPlanner{Result: result, Prefix: p.getPrefix()}, nil
}

// metricsNestedSet lists the stored columns of the nested-set fields fn reads
// as its argument or by-labels, each once; none without fn.
func metricsNestedSet(fn *traceql_ir.MetricsFn) []nestedSetCol {
	if fn == nil {
		return nil
	}
	var cols []nestedSetCol
	for _, f := range append([]string{fn.Attr}, fn.By...) {
		if col, ok := nestedSetCols[f]; ok && !slices.Contains(cols, col) {
			cols = append(cols, col)
		}
	}
	return cols
}

// metricsSpansPlanner reads the traces table for the result spans of Result,
// an evaluated pipeline: one row per span, as MetricsBuild reads, with the
// stored values of the NestedSet columns.
type metricsSpansPlanner struct {
	Result    shared.SQLRequestPlanner
	Prefix    string
	NestedSet []nestedSetCol
}

func (m *metricsSpansPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	withResult, withIds, err := resultSpanIds(ctx, m.Result, m.Prefix)
	if err != nil {
		return nil, err
	}
	tsFrom, tsTo := timerange.SpanStart("traces.timestamp_ns", ctx.From.UnixNano(), ctx.To.UnixNano())
	spans := sql.NewSelect().
		Select(
			sql.NewSimpleCol("traces.trace_id", "trace_id"),
			sql.NewSimpleCol("traces.span_id", "span_id"),
			sql.NewSimpleCol("any(traces.timestamp_ns)", "timestamp_ns"),
			sql.NewSimpleCol("any(traces.duration_ns)", "duration_ns"),
			sql.NewSimpleCol("any(traces.name)", "name"),
			sql.NewSimpleCol("any(traces.service_name)", "service_name")).
		From(sql.NewSimpleCol(tracesTable(ctx), "traces")).
		AndWhere(
			tsFrom,
			tsTo,
			inSubquery(ctx, sql.NewRawObject("traces.trace_id"), traceIdsOf(withIds)),
			inSubquery(ctx, sql.NewRawObject("(traces.trace_id, traces.span_id)"), sql.NewWithRef(withIds))).
		GroupBy(sql.NewRawObject("traces.trace_id"), sql.NewRawObject("traces.span_id"))
	if len(m.NestedSet) == 0 {
		return spans.With(withResult, withIds), nil
	}
	withSpans := sql.NewWith(spans, m.Prefix+"metrics_traces")
	s := withSpans.GetAlias()
	ns := joinNestedSet(ctx, m.NestedSet, traceIdsOf(withIds), sql.NewWithRef(withIds), s,
		m.Prefix+"metrics_nested_set", nestedSetCol.metrics)
	cols := columnsOf(s, "trace_id", "span_id", "timestamp_ns", "duration_ns", "name", "service_name")
	return sql.NewSelect().
		With(withResult, withIds, withSpans, ns.with).
		Select(append(cols, ns.cols...)...).
		From(sql.NewWithRef(withSpans)).
		Join(ns.join), nil
}

// resultSpanIds evaluates result, a pipeline, and returns its span rows and
// their (trace_id, span_id) pairs.
func resultSpanIds(ctx *shared.PlannerContext, result shared.SQLRequestPlanner, prefix string) (
	withResult *sql.With, withIds *sql.With, err error) {
	main, err := result.Process(ctx)
	if err != nil {
		return nil, nil, err
	}
	withResult = sql.NewWith(main, prefix+"result_spans")
	ids := sql.NewSelect().
		Select(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id")).
		From(sql.NewWithRef(withResult))
	return withResult, sql.NewWith(ids, prefix+"result_span_ids"), nil
}

// metricsPlanner builds a metrics query over the metrics spans, whose
// operand reads keep only Sample's rows when it is set.
type metricsPlanner struct {
	Spans  *metricsSpansPlanner
	Sample sql.SQLCondition
	Build  MetricsBuild
}

func (m *metricsPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	spans, err := m.Spans.Process(sampled(ctx, m.Sample))
	if err != nil {
		return nil, err
	}
	return m.Build(sql.NewWith(spans, "metrics_spans"))
}

// comparePlanner builds compare() over the baseline spans, taken over the
// baseline window, and the selection spans, nil for `{}`. Both keep only
// Sample's rows when it is set.
type comparePlanner struct {
	Baseline                 *metricsSpansPlanner
	Selection                *metricsSpansPlanner
	BaselineFrom, BaselineTo time.Time
	Sample                   sql.SQLCondition
	Build                    CompareBuild
}

func (c *comparePlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	ctx = sampled(ctx, c.Sample)
	baselineCtx := *ctx
	baselineCtx.From, baselineCtx.To = c.BaselineFrom, c.BaselineTo
	baseline, err := c.Baseline.Process(&baselineCtx)
	if err != nil {
		return nil, err
	}
	var selection *sql.With
	if c.Selection != nil {
		spans, err := c.Selection.Process(ctx)
		if err != nil {
			return nil, err
		}
		selection = sql.NewWith(spans, "selection_spans")
	}
	return c.Build(sql.NewWith(baseline, "baseline_spans"), selection)
}
