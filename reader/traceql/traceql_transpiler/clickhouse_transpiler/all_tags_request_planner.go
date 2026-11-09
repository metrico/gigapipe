package clickhouse_transpiler

import (
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/traceql/timerange"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

type AllTagsRequestPlanner struct {
}

func (a *AllTagsRequestPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	dateFrom, dateTo := timerange.SpanDate("date", ctx.From, ctx.To)
	return sql.NewSelect().
		Distinct(true).
		Select(sql.NewSimpleCol("key", "key")).
		From(sql.NewRawObject(ctx.TracesKVDistTable)).
		AndWhere(dateFrom, dateTo), nil
}
