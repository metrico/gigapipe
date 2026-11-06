package clickhouse_transpiler

import (
	"fmt"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/traceql/timerange"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// SpanRead starts a statement over span rows aliased main.
type SpanRead func() sql.ISelect

// ReadBuild builds a statement over read.
type ReadBuild func(read SpanRead) (sql.ISelect, error)

// ShardBuild builds a statement in two steps.
type ShardBuild struct {
	// Shard is one shard's statement over read and that shard's attribute table.
	Shard func(read SpanRead, attrs string) (sql.ISelect, error)
	// Result reads the rows of all the shards.
	Result func(rows *sql.With) (sql.ISelect, error)
}

// EverySpan reports whether m reads every span in range: a lone `{}` with no
// nested-set field.
func (m *MetricsPlan) EverySpan() bool {
	if m.spans == nil || m.spans.OnePass == nil || len(m.spans.NestedSet) > 0 {
		return false
	}
	return m.spans.OnePass.Result == nil && m.spans.OnePass.Filter == nil
}

// OverShards is a statement built by build, with its Shard run on each shard
// over the traces rows; m is an EverySpan plan.
func (m *MetricsPlan) OverShards(build ShardBuild) shared.SQLRequestPlanner {
	return &everySpanShards{build: build, read: func(ctx *shared.PlannerContext) SpanRead {
		return everySpanRead(ctx, m.mainSample)
	}}
}

// OverDistinctSpanShards is OverShards over one row per span hash with cols.
func (m *MetricsPlan) OverDistinctSpanShards(cols []string, build ShardBuild) shared.SQLRequestPlanner {
	distinct := &everySpanDistinct{sample: m.mainSample, cols: cols}
	return &everySpanShards{build: build, read: func(ctx *shared.PlannerContext) SpanRead {
		return overCTE(distinct.spans(ctx))
	}}
}

type everySpanShards struct {
	build ShardBuild
	read  func(ctx *shared.PlannerContext) SpanRead
}

func (e *everySpanShards) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	if !ctx.IsCluster {
		rows, err := e.build.Shard(e.read(ctx), attrsTable(ctx))
		if err != nil {
			return nil, err
		}
		return e.build.Result(sql.NewWith(rows, "shard_rows"))
	}
	local := localContext(ctx)
	shard, err := e.build.Shard(e.read(local), attrsTable(local))
	if err != nil {
		return nil, err
	}
	rows, err := selectOnEachShard(ctx, shard, "*")
	if err != nil {
		return nil, err
	}
	return e.build.Result(sql.NewWith(clusterSettings(ctx, rows), "shard_rows"))
}

// OverRows is a statement built by build over the traces rows, a copy of a span
// being a row of its own; m is an EverySpan plan.
func (m *MetricsPlan) OverRows(build ReadBuild) shared.SQLRequestPlanner {
	return &everySpanRows{sample: m.mainSample, build: build}
}

type everySpanRows struct {
	sample sql.SQLCondition
	build  ReadBuild
}

func (e *everySpanRows) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	return e.build(everySpanRead(ctx, e.sample))
}

// OverDistinctSpans is a statement over one row per span hash with cols, m being
// an EverySpan plan: on a cluster its own table function, or a CTE with skipped shards.
func (m *MetricsPlan) OverDistinctSpans(cols []string, build ReadBuild) shared.SQLRequestPlanner {
	return &everySpanDistinct{sample: m.mainSample, cols: cols, build: build}
}

type everySpanDistinct struct {
	sample sql.SQLCondition
	cols   []string
	build  ReadBuild
}

func (e *everySpanDistinct) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	if !ctx.IsCluster {
		return e.build(overCTE(e.spans(ctx)))
	}
	shard := e.spans(localContext(ctx))
	if !ctx.SkipUnavailableShards {
		view, err := clusterView(ctx, shard)
		if err != nil {
			return nil, err
		}
		return e.build(func() sql.ISelect {
			return sql.NewSelect().From(sql.NewCol(view, "main"))
		})
	}
	rows, err := selectOnEachShard(ctx, shard, "*")
	if err != nil {
		return nil, err
	}
	return e.build(overCTE(clusterSettings(ctx, rows)))
}

// spans reads one row per span hash of ctx's traces table.
func (e *everySpanDistinct) spans(ctx *shared.PlannerContext) sql.ISelect {
	cols := make([]sql.SQLObject, len(e.cols))
	for i, col := range e.cols {
		cols[i] = sql.NewSimpleCol(fmt.Sprintf("any(main.%s)", col), col)
	}
	return everySpanRead(ctx, e.sample)().
		Select(cols...).
		GroupBy(sql.NewRawObject("cityHash64(main.trace_id, main.span_id)"))
}

// overCTE reads spans through a CTE, as main.
func overCTE(spans sql.ISelect) SpanRead {
	with := sql.NewWith(spans, "metrics_spans")
	return func() sql.ISelect {
		return sql.NewSelect().With(with).From(sql.NewCol(sql.NewWithRef(with), "main"))
	}
}

// everySpanRead reads the rows of ctx's traces table that start in its range,
// in the sample, as main. Every row counts, so a span stored twice has two.
func everySpanRead(ctx *shared.PlannerContext, sample sql.SQLCondition) SpanRead {
	return func() sql.ISelect {
		tsFrom, tsTo := timerange.SpanStart("main.timestamp_ns", ctx.From.UnixNano(), ctx.To.UnixNano())
		read := sql.NewSelect().
			From(sql.NewSimpleCol(tracesTable(ctx), "main")).
			AndWhere(tsFrom, tsTo)
		restrictOperand(sampled(ctx, sample), read)
		return read
	}
}
