package clickhouse_transpiler

import (
	"fmt"
	"strings"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/metrico/qryn/v5/shared/distconfig"
)

// shardLocalPlanner runs Shard on each shard over local tables and keeps the
// ctx.Limit newest traces; a trace's spans and attribute rows must live on
// one shard. Every shard, the local one included, gets Shard as a remote
// query so all shards run at once.
type shardLocalPlanner struct {
	Shard  shared.SQLRequestPlanner
	Single shared.SQLRequestPlanner
}

func (s *shardLocalPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	if !ctx.IsCluster {
		return s.Single.Process(ctx)
	}
	if ctx.ClusterName == "" {
		return nil, fmt.Errorf("shard-local statement on a cluster without a cluster name")
	}
	shard, err := s.Shard.Process(localContext(ctx))
	if err != nil {
		return nil, err
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
	res := sql.NewSelect().
		Select(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id"), sql.NewRawObject("spansets")).
		From(view).
		OrderBy(
			sql.NewOrderBy(sql.NewRawObject("trace_newest"), sql.ORDER_BY_DIRECTION_DESC),
			sql.NewOrderBy(sql.NewRawObject("trace_id"), sql.ORDER_BY_DIRECTION_DESC)).
		SetSetting("prefer_localhost_replica", "0")
	if ctx.Limit > 0 {
		res = res.Limit(sql.NewIntVal(ctx.Limit))
	}
	return res, nil
}

// localContext is ctx planning a single shard's reads: each per-trace table,
// local or distributed, names the shard's local table. Tag tables, sharded by
// key, stay distributed.
func localContext(ctx *shared.PlannerContext) *shared.PlannerContext {
	res := *ctx
	res.IsCluster = false
	for _, t := range []struct{ local, dist *string }{
		{&res.TracesTable, &res.TracesDistTable},
		{&res.TracesAttrsTable, &res.TracesAttrsDistTable},
		{&res.TracesNestedSetTable, &res.TracesNestedSetDistTable},
	} {
		*t.local = strings.TrimSuffix(*t.dist, distconfig.Suffix())
		*t.dist = *t.local
	}
	return &res
}
