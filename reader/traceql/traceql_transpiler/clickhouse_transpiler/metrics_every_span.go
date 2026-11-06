package clickhouse_transpiler

import (
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/traceql/timerange"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// SpanRead starts a statement over span rows aliased main.
type SpanRead func() sql.ISelect

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

// OverShards is a statement built by build, with its Shard run on each shard;
// m is an EverySpan plan.
func (m *MetricsPlan) OverShards(build ShardBuild) shared.SQLRequestPlanner {
	return &everySpanShards{sample: m.mainSample, build: build}
}

type everySpanShards struct {
	sample sql.SQLCondition
	build  ShardBuild
}

func (e *everySpanShards) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	if !ctx.IsCluster {
		rows, err := e.build.Shard(everySpanRead(ctx, e.sample), attrsTable(ctx))
		if err != nil {
			return nil, err
		}
		return e.build.Result(sql.NewWith(rows, "span_counts"))
	}
	local := localContext(ctx)
	shard, err := e.build.Shard(everySpanRead(local, e.sample), attrsTable(local))
	if err != nil {
		return nil, err
	}
	rows, err := selectOnEachShard(ctx, shard, "*")
	if err != nil {
		return nil, err
	}
	return e.build.Result(sql.NewWith(clusterSettings(ctx, rows), "span_counts"))
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
