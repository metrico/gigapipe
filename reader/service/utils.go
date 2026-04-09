package service

import (
	"time"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/clickhouse_planner"
	"github.com/metrico/qryn/v4/reader/model"
	"github.com/metrico/qryn/v4/shared/distconfig"
)

func getTableName(ctx *model.DataDatabasesMap, name string) string {
	if ctx.Config.ClusterName != "" {
		return name + distconfig.Suffix()
	}
	return name
}

func FormatFromDate(from time.Time) string {
	return clickhouse_planner.FormatFromDate(from)
}
