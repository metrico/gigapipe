package clickhouse_transpiler

import (
	"fmt"
	"strings"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// shardLocalPlanner runs Shard on each shard over local tables and returns
// Cols; on a single node it runs Single.
type shardLocalPlanner struct {
	Shard  shared.SQLRequestPlanner
	Single shared.SQLRequestPlanner
	Cols   []string
	// Newest keeps the ctx.Limit traces with the greatest trace_newest.
	Newest bool
}

// newestShardLocal is the search over main's spansets, keeping the newest
// traces of each shard and then of all shards.
func newestShardLocal(main shared.SQLRequestPlanner) *shardLocalPlanner {
	return &shardLocalPlanner{
		Shard:  &IndexLimitPlanner{&SearchSpansetPlanner{Main: main, Newest: true}},
		Single: &IndexLimitPlanner{&SearchSpansetPlanner{Main: main}},
		Cols:   []string{"trace_id", "span_id", "spansets"},
		Newest: true,
	}
}

// shardLocal runs ids on each shard, or on one node, and returns cols.
func shardLocal(ids shared.SQLRequestPlanner, cols ...string) *shardLocalPlanner {
	return &shardLocalPlanner{Shard: ids, Single: ids, Cols: cols}
}

func (s *shardLocalPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	if !ctx.IsCluster {
		return s.Single.Process(ctx)
	}
	shard, err := s.Shard.Process(localContext(ctx))
	if err != nil {
		return nil, err
	}
	res, err := onEachShard(ctx, shard, s.Cols...)
	if err != nil {
		return nil, err
	}
	if !s.Newest {
		return res, nil
	}
	res = res.OrderBy(
		sql.NewOrderBy(sql.NewRawObject("trace_newest"), sql.ORDER_BY_DIRECTION_DESC),
		sql.NewOrderBy(sql.NewRawObject("trace_id"), sql.ORDER_BY_DIRECTION_DESC))
	if ctx.Limit > 0 {
		res = res.Limit(sql.NewIntVal(ctx.Limit))
	}
	return res, nil
}

// onEachShard selects cols of shard, a statement over localContext(ctx), run on
// every shard of ctx's cluster, the local one included.
func onEachShard(ctx *shared.PlannerContext, shard sql.ISelect, cols ...string) (sql.ISelect, error) {
	if ctx.ClusterName == "" {
		return nil, fmt.Errorf("shard-local statement on a cluster without a cluster name")
	}
	view := sql.NewCustomCol(func(c *sql.Ctx, options ...int) (string, error) {
		name, err := sql.NewStringVal(ctx.ClusterName).String(c)
		if err != nil {
			return "", err
		}
		body, err := shard.String(c)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("cluster(%s, view(%s))", name, body), nil
	})
	sel := make([]sql.SQLObject, len(cols))
	for i, c := range cols {
		sel[i] = sql.NewRawObject(c)
	}
	return sql.NewSelect().Select(sel...).From(view).SetSetting("prefer_localhost_replica", "0"), nil
}

// clusterSettings sets stmt's statement-level settings for ctx's cluster:
// unavailable shards are skipped when the read tables skip them.
func clusterSettings(ctx *shared.PlannerContext, stmt sql.ISelect) sql.ISelect {
	if ctx.IsCluster && ctx.SkipUnavailableShards {
		return stmt.SetSetting("skip_unavailable_shards", "1")
	}
	return stmt
}

// localContext is ctx planning one shard's reads: each per-trace table names
// the shard's local table; tag tables, sharded by key, stay distributed.
func localContext(ctx *shared.PlannerContext) *shared.PlannerContext {
	res := *ctx
	res.IsCluster = false
	for _, t := range []struct{ local, dist *string }{
		{&res.TracesTable, &res.TracesDistTable},
		{&res.TracesAttrsTable, &res.TracesAttrsDistTable},
		{&res.TracesNestedSetTable, &res.TracesNestedSetDistTable},
	} {
		*t.local = strings.TrimSuffix(*t.dist, ctx.DistSuffix)
		*t.dist = *t.local
	}
	return &res
}
