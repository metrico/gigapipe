package service

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/clickhouse_planner"
	"github.com/metrico/qryn/reader/model"
	"time"
)

func getTableName(ctx *model.DataDatabasesMap, name string) string {
	if ctx.Config.ClusterName != "" {
		return name + "_dist"
	}
	return name
}

func FormatFromDate(from time.Time) string {
	return clickhouse_planner.FormatFromDate(from)
}
