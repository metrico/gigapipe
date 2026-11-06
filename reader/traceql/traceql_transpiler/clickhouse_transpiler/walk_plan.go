package clickhouse_transpiler

import (
	"fmt"
	"strings"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/traceql/timerange"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// allSpansLeaf reads the trace of every span starting in range.
type allSpansLeaf struct{}

func (allSpansLeaf) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	tsFrom, tsTo := timerange.SpanStart("timestamp_ns", ctx.From.UnixNano(), ctx.To.UnixNano())
	return sql.NewSelect().
		Select(sql.NewSimpleCol("trace_id", "trace_id")).
		From(sql.NewSimpleCol(tracesTable(ctx), "traces")).
		AndWhere(tsFrom, tsTo), nil
}

// candidateLeaves plans a read of each leaf selector of pipeline. A pipeline
// reading every span makes every span in range a match, and so does a `{}`.
func (p *planner) candidateLeaves(pipeline *traceql_ir.Pipeline) ([]shared.SQLRequestPlanner, error) {
	leaves, err := p.pipelineLeaves(pipeline)
	if err != nil {
		return nil, err
	}
	for _, leaf := range leaves {
		if _, all := leaf.(allSpansLeaf); all {
			return []shared.SQLRequestPlanner{leaf}, nil
		}
	}
	return leaves, nil
}

func (p *planner) pipelineLeaves(pipeline *traceql_ir.Pipeline) ([]shared.SQLRequestPlanner, error) {
	if readsEverySpan(pipeline) {
		return []shared.SQLRequestPlanner{allSpansLeaf{}}, nil
	}
	var res []shared.SQLRequestPlanner
	for _, element := range pipeline.Elements {
		e, ok := element.(traceql_ir.SpansetExpr)
		if !ok {
			continue
		}
		leaves, err := planSpansetTree(p, e,
			func(op *simpleExpressionPlanner) ([]shared.SQLRequestPlanner, error) {
				if op.selector.Filter == nil {
					return []shared.SQLRequestPlanner{allSpansLeaf{}}, nil
				}
				leaf, err := op.planner()
				return []shared.SQLRequestPlanner{leaf}, err
			},
			p.pipelineLeaves,
			func(_ *traceql_ir.SpansetOp, lhs, rhs []shared.SQLRequestPlanner) []shared.SQLRequestPlanner {
				return append(lhs, rhs...)
			})
		if err != nil {
			return nil, err
		}
		res = append(res, leaves...)
	}
	return res, nil
}

// candidatesPlanner returns the distinct trace ids of leaves over (from, to].
type candidatesPlanner struct {
	leaves   []shared.SQLRequestPlanner
	from, to time.Time
}

func (c *candidatesPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	leafCtx := *ctx
	leafCtx.From, leafCtx.To = c.from, c.to
	leafCtx.TracesCandidates, leafCtx.TracesOperandSpans = nil, nil
	reads := make([]sql.ISelect, len(c.leaves))
	for i, leaf := range c.leaves {
		read, err := leaf.Process(&leafCtx)
		if err != nil {
			return nil, err
		}
		reads[i] = read
	}
	union := sql.NewCustomCol(func(sc *sql.Ctx, options ...int) (string, error) {
		parts := make([]string, len(reads))
		for i, read := range reads {
			s, err := read.String(sc)
			if err != nil {
				return "", err
			}
			parts[i] = fmt.Sprintf("SELECT trace_id FROM (%s)", s)
		}
		return "(" + strings.Join(parts, " UNION DISTINCT ") + ")", nil
	})
	return sql.NewSelect().Select(sql.NewRawObject("trace_id")).From(union), nil
}

// candidatesFilter keeps the rows of ctx.TracesCandidates' traces, or is nil
// when every trace is a candidate.
func candidatesFilter(ctx *shared.PlannerContext) sql.SQLCondition {
	if ctx.TracesCandidates == nil {
		return nil
	}
	ids := sql.NewCustomCol(func(sc *sql.Ctx, options ...int) (string, error) {
		sel, err := ctx.TracesCandidates.Process(ctx)
		if err != nil {
			return "", err
		}
		return sel.String(sc)
	})
	return inSubquery(ctx, sql.NewRawObject("trace_id"), ids)
}

// ExtentPlanner returns, per shard, the earliest and latest start and the count
// of the spans in range of ctx.TracesCandidates' traces.
type ExtentPlanner struct{}

func (e *ExtentPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	tsFrom, tsTo := timerange.SpanStart("timestamp_ns", ctx.From.UnixNano(), ctx.To.UnixNano())
	return sql.NewSelect().
		Select(
			sql.NewSimpleCol("toInt64(min(timestamp_ns))", "lo"),
			sql.NewSimpleCol("toInt64(max(timestamp_ns))", "hi"),
			sql.NewSimpleCol("toInt64(count())", "spans"),
			sql.NewSimpleCol("shardNum()", "shard")).
		From(sql.NewSimpleCol(tracesTable(ctx), "traces")).
		AndWhere(tsFrom, tsTo, candidatesFilter(ctx)).
		GroupBy(sql.NewRawObject("shard")), nil
}

// ShardExtent is a row of ExtentPlanner.
type ShardExtent struct {
	Lo, Hi, Spans int64
	Shard         uint32
}

// ReadExtents runs ExtentPlanner: no rows when ctx.TracesCandidates has no spans in range.
func ReadExtents(ctx *shared.PlannerContext) ([]ShardExtent, error) {
	str, err := render(ctx, &ExtentPlanner{})
	if err != nil {
		return nil, err
	}
	rows, err := ctx.CHDb.QueryCtx(ctx.Ctx, str)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []ShardExtent
	for rows.Next() {
		var e ShardExtent
		if err := rows.Scan(&e.Lo, &e.Hi, &e.Spans, &e.Shard); err != nil {
			return nil, err
		}
		res = append(res, e)
	}
	return res, rows.Err()
}
