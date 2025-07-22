package service

import (
	"time"

	"github.com/metrico/qryn/reader/logql/transpiler/clickhouse_planner"
	"github.com/metrico/qryn/reader/model"
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
