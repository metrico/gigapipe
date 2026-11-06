package clickhouse_transpiler

import (
	"errors"
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

// CompareBuild builds compare() over CTEs shaped like MetricsBuild's.
type CompareBuild struct {
	// Counts counts attribute values of baseline spans, split by selection (nil
	// for `{}`), over traceIds and attrs; on a cluster it runs on each shard.
	Counts func(baseline, selection, traceIds *sql.With, attrs string) (sql.ISelect, error)
	// Result sums the counts and keeps the topN values per attribute.
	Result func(counts *sql.With) (sql.ISelect, error)
}

// MetricsPlan is a metrics query in steps: each Ids statement lists span ids,
// and Metrics reads the spans of those ids from the Ids external tables. A side
// that relates no spans across a trace has no Ids statement: Metrics finds its
// spans itself.
type MetricsPlan struct {
	Ids     []IdsStatement
	Metrics shared.SQLRequestPlanner
	// spans are a PlanMetrics plan's spans, nil for compare().
	spans *metricsSpansPlanner
}

// PlanMetrics plans a metrics query over the result spans of the query's
// spanset expression in the query range: the spans a search for it returns,
// uncapped. A query that needs the whole trace lists them into SearchResultTable.
func PlanMetrics(query *traceql_ir.Query, build MetricsBuild) (*MetricsPlan, error) {
	p := &planner{query: query}
	result, err := p.planQuery()
	if err != nil {
		return nil, err
	}
	spans := &metricsSpansPlanner{Prefix: p.getPrefix(), NestedSet: metricsNestedSet(query.Metrics)}
	plan := &MetricsPlan{Metrics: &metricsPlanner{Spans: spans, Build: build}, spans: spans}
	if query.NeedsWholeTrace() {
		spans.Ids = SearchResultTable
		plan.Ids = []IdsStatement{{
			Table:     SearchResultTable,
			Statement: p.structuralIds(&spanIdsPlanner{Result: result, Sample: sampleFilter(query)}),
		}}
	} else {
		spans.OnePass = onePassOf(&query.Pipeline, result, sampleFilter(query))
	}
	return plan, nil
}

// Over is a metrics statement built by build over m's spans, like m.Metrics;
// m is a PlanMetrics plan.
func (m *MetricsPlan) Over(build MetricsBuild) (shared.SQLRequestPlanner, error) {
	if m.spans == nil {
		return nil, errors.New("compare() has no metrics statement over its ids")
	}
	return &metricsPlanner{Spans: m.spans, Build: build}, nil
}

// PlanCompare plans compare() over the baseline, the result spans in [baselineFrom,
// baselineTo], and the selection filter's spans. A baseline that needs the whole
// trace lists its spans into SearchResultTable.
func PlanCompare(query *traceql_ir.Query, selection traceql_ir.FieldExpr,
	baselineFrom, baselineTo time.Time, build CompareBuild) (*MetricsPlan, error) {
	p := &planner{query: query}
	result, err := p.planQuery()
	if err != nil {
		return nil, err
	}
	sample := sampleFilter(query)
	res := &MetricsPlan{}
	cmp := &comparePlanner{Baseline: &metricsSpansPlanner{Prefix: p.getPrefix()},
		BaselineFrom: baselineFrom, BaselineTo: baselineTo, Build: build}
	if query.NeedsWholeTrace() {
		baseline := &windowPlanner{Main: &spanIdsPlanner{Result: result, Sample: sample}, From: baselineFrom, To: baselineTo}
		res.Ids = []IdsStatement{{Table: SearchResultTable, Statement: p.structuralIds(baseline)}}
		cmp.Baseline.Ids = SearchResultTable
	} else {
		cmp.Baseline.OnePass = onePassOf(&query.Pipeline, result, sample)
	}
	if selection != nil {
		expr, err := p.operand(&traceql_ir.Selector{Filter: selection}).planner()
		if err != nil {
			return nil, err
		}
		filter := &pipelinePlanner{
			Stages: []pipelineStage{&spansetStage{Expr: expr}},
			Prefix: p.getPrefix(),
		}
		selector := &traceql_ir.Pipeline{Elements: []traceql_ir.Element{&traceql_ir.Selector{Filter: selection}}}
		cmp.Selection = &metricsSpansPlanner{OnePass: onePassOf(selector, filter, sample), Prefix: p.getPrefix()}
	}
	res.Metrics = cmp
	return res, nil
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

// SampleScale is the factor that scales a sampled volume up to the whole: 1/f
// for the query's sample hint f, or 1 without a hint or when its hash threshold
// is 0, which keeps no spans.
func SampleScale(query *traceql_ir.Query) float64 {
	f, ok := query.SampleFraction()
	if !ok || sampleThreshold(f) == 0 {
		return 1
	}
	return 1 / f
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

// metricsSpansPlanner reads the traces table for the spans of OnePass, or else
// of the ids table Ids: one row per span, as MetricsBuild reads, with the stored
// values of the NestedSet columns.
type metricsSpansPlanner struct {
	Ids       string
	OnePass   *onePassRead
	Prefix    string
	NestedSet []nestedSetCol
}

func (m *metricsSpansPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	if m.OnePass != nil {
		return m.onePassOnEachShard(ctx)
	}
	withRows, withTraceIds, withSpanIds := idsOf(m.Ids, m.Prefix)
	spans := metricsTracesRead(ctx).
		AndWhere(
			plainIn(sql.NewRawObject("traces.trace_id"), sql.NewWithRef(withTraceIds)),
			plainIn(sql.NewRawObject("(traces.trace_id, traces.span_id)"), sql.NewWithRef(withSpanIds)))
	if len(m.NestedSet) == 0 {
		return spans.With(withRows, withTraceIds, withSpanIds), nil
	}
	withSpans := sql.NewWith(spans, m.Prefix+"metrics_traces")
	s := withSpans.GetAlias()
	ns := joinNestedSet(ctx, m.NestedSet, plainIn, sql.NewWithRef(withTraceIds), sql.NewWithRef(withSpanIds), s,
		m.Prefix+"metrics_nested_set", nestedSetCol.metrics)
	cols := columnsOf(s, "trace_id", "span_id", "timestamp_ns", "duration_ns", "name", "service_name")
	return sql.NewSelect().
		With(withRows, withTraceIds, withSpanIds, withSpans, ns.with).
		Select(append(cols, ns.cols...)...).
		From(sql.NewWithRef(withSpans)).
		Join(ns.join), nil
}

// metricsTracesRead is one row per span of the traces table that starts in ctx's range.
func metricsTracesRead(ctx *shared.PlannerContext) sql.ISelect {
	tsFrom, tsTo := timerange.SpanStart("traces.timestamp_ns", ctx.From.UnixNano(), ctx.To.UnixNano())
	return sql.NewSelect().
		Select(
			sql.NewSimpleCol("traces.trace_id", "trace_id"),
			sql.NewSimpleCol("traces.span_id", "span_id"),
			sql.NewSimpleCol("any(traces.timestamp_ns)", "timestamp_ns"),
			sql.NewSimpleCol("any(traces.duration_ns)", "duration_ns"),
			sql.NewSimpleCol("any(traces.name)", "name"),
			sql.NewSimpleCol("any(traces.service_name)", "service_name")).
		From(sql.NewSimpleCol(tracesTable(ctx), "traces")).
		AndWhere(tsFrom, tsTo).
		GroupBy(sql.NewRawObject("traces.trace_id"), sql.NewRawObject("traces.span_id"))
}

// metricsPlanner builds a metrics query over the metrics spans.
type metricsPlanner struct {
	Spans *metricsSpansPlanner
	Build MetricsBuild
}

func (m *metricsPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	spans, err := m.Spans.Process(ctx)
	if err != nil {
		return nil, err
	}
	res, err := m.Build(sql.NewWith(spans, "metrics_spans"))
	if err != nil {
		return nil, err
	}
	if m.Spans.OnePass != nil {
		return clusterSettings(ctx, res), nil
	}
	return res, nil
}

// comparePlanner builds compare() over the baseline spans, taken over the
// baseline window, and the selection spans, nil for `{}`. On a cluster each
// shard counts its own traces and the coordinator only merges the counts.
type comparePlanner struct {
	Baseline                 *metricsSpansPlanner
	Selection                *metricsSpansPlanner
	BaselineFrom, BaselineTo time.Time
	Build                    CompareBuild
}

func (c *comparePlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	if !ctx.IsCluster {
		counts, err := c.counts(ctx)
		if err != nil {
			return nil, err
		}
		return c.Build.Result(sql.NewWith(counts, "compare_counts"))
	}
	shard, err := c.counts(localContext(ctx))
	if err != nil {
		return nil, err
	}
	counts, err := onEachShard(ctx, shard, "attr_key", "attr_val", "sel_n", "base_n")
	if err != nil {
		return nil, err
	}
	res, err := c.Build.Result(sql.NewWith(counts, "compare_counts"))
	if err != nil {
		return nil, err
	}
	return clusterSettings(ctx, res), nil
}

// counts is Build.Counts over ctx's tables.
func (c *comparePlanner) counts(ctx *shared.PlannerContext) (sql.ISelect, error) {
	baseline, err := (&windowPlanner{Main: c.Baseline, From: c.BaselineFrom, To: c.BaselineTo}).Process(ctx)
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
	withBaseline := sql.NewWith(baseline, "baseline_spans")
	if c.Baseline.OnePass != nil {
		traceIds := sql.NewWith(traceIdsOf(withBaseline), c.Baseline.Prefix+"trace_ids")
		return c.Build.Counts(withBaseline, selection, traceIds, attrsTable(ctx))
	}
	rows, traceIds, _ := idsOf(c.Baseline.Ids, c.Baseline.Prefix)
	traceIds.GetQuery().AddWith(rows)
	return c.Build.Counts(withBaseline, selection, traceIds, attrsTable(ctx))
}
