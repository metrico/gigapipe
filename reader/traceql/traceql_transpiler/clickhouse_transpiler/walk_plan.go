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

// allSpansLeaf reads the trace and start of every span starting in range.
type allSpansLeaf struct{}

func (allSpansLeaf) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	tsFrom, tsTo := timerange.SpanStart("timestamp_ns", ctx.From.UnixNano(), ctx.To.UnixNano())
	return sql.NewSelect().
		Select(sql.NewSimpleCol("trace_id", "trace_id"), sql.NewSimpleCol("timestamp_ns", "timestamp_ns")).
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
	union, err := leafUnion(ctx, c.leaves, c.from, c.to, "trace_id", true)
	if err != nil {
		return nil, err
	}
	return sql.NewSelect().Select(sql.NewRawObject("trace_id")).From(union), nil
}

// leafUnion selects column from every leaf's read over (from, to], joined by
// UNION DISTINCT when distinct, else UNION ALL.
func leafUnion(ctx *shared.PlannerContext, leaves []shared.SQLRequestPlanner, from, to time.Time,
	column string, distinct bool) (sql.SQLObject, error) {
	union := "UNION ALL"
	if distinct {
		union = "UNION DISTINCT"
	}
	leafCtx := *ctx
	leafCtx.From, leafCtx.To = from, to
	leafCtx.TracesCandidates, leafCtx.TracesOperandSpans = nil, nil
	reads := make([]sql.ISelect, len(leaves))
	for i, leaf := range leaves {
		read, err := leaf.Process(&leafCtx)
		if err != nil {
			return nil, err
		}
		reads[i] = read
	}
	return sql.NewCustomCol(func(sc *sql.Ctx, options ...int) (string, error) {
		parts := make([]string, len(reads))
		for i, read := range reads {
			s, err := read.String(sc)
			if err != nil {
				return "", err
			}
			parts[i] = fmt.Sprintf("SELECT %s FROM (%s)", column, s)
		}
		return "(" + strings.Join(parts, " "+union+" ") + ")", nil
	}), nil
}

// newestCandidatePlanner returns the latest start and the count of the leaf
// matches over (from, to].
type newestCandidatePlanner struct {
	leaves   []shared.SQLRequestPlanner
	from, to time.Time
}

func (n *newestCandidatePlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	union, err := leafUnion(ctx, n.leaves, n.from, n.to, "timestamp_ns", false)
	if err != nil {
		return nil, err
	}
	return sql.NewSelect().
		Select(
			sql.NewSimpleCol("toInt64(max(timestamp_ns))", "newest"),
			sql.NewSimpleCol("toInt64(count())", "matches")).
		From(union), nil
}

// ReadNewestCandidate runs planner, a SearchPlan.NewestCandidate: the newest
// match's start in nanoseconds, false when there is none.
func ReadNewestCandidate(ctx *shared.PlannerContext, planner shared.SQLRequestPlanner) (int64, bool, error) {
	str, err := render(ctx, planner)
	if err != nil {
		return 0, false, err
	}
	rows, err := ctx.CHDb.QueryCtx(ctx.Ctx, str)
	if err != nil {
		return 0, false, err
	}
	defer rows.Close()
	var newest, matches int64
	if rows.Next() {
		if err := rows.Scan(&newest, &matches); err != nil {
			return 0, false, err
		}
	}
	return newest, matches > 0, rows.Err()
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

// ExtentPlanner returns, per shard, the earliest and latest start and the count of
// the spans in range of ctx.TracesCandidates' traces, and how many traces hold them.
type ExtentPlanner struct{}

func (e *ExtentPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	tsFrom, tsTo := timerange.SpanStart("timestamp_ns", ctx.From.UnixNano(), ctx.To.UnixNano())
	return sql.NewSelect().
		Select(
			sql.NewSimpleCol("toInt64(min(timestamp_ns))", "lo"),
			sql.NewSimpleCol("toInt64(max(timestamp_ns))", "hi"),
			sql.NewSimpleCol("toInt64(count())", "spans"),
			sql.NewSimpleCol("toInt64(uniqExact(trace_id))", "trace_count"),
			sql.NewSimpleCol("shardNum()", "shard")).
		From(sql.NewSimpleCol(tracesTable(ctx), "traces")).
		AndWhere(tsFrom, tsTo, candidatesFilter(ctx)).
		GroupBy(sql.NewRawObject("shard")), nil
}

// ShardExtent is a row of ExtentPlanner.
type ShardExtent struct {
	Lo, Hi, Spans, Traces int64
	Shard                 uint32
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
		if err := rows.Scan(&e.Lo, &e.Hi, &e.Spans, &e.Traces, &e.Shard); err != nil {
			return nil, err
		}
		res = append(res, e)
	}
	return res, rows.Err()
}
