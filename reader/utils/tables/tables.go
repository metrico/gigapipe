package tables

import (
	"fmt"
	"sync"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v4/reader/model"
	"github.com/metrico/qryn/v4/reader/plugins"
)

var tableNames = func() map[string]string {
	return map[string]string{}
}()
var lock sync.RWMutex

func init() {
	lock.Lock()
	defer lock.Unlock()

	tableNames["tempo_traces"] = "tempo_traces"
	tableNames["tempo_traces_dist"] = "tempo_traces_dist"
	tableNames["tempo_traces_kv"] = "tempo_traces_kv"
	tableNames["tempo_traces_kv_dist"] = "tempo_traces_kv_dist"
	tableNames["time_series"] = "time_series"
	tableNames["time_series_dist"] = "time_series_dist"
	tableNames["samples_kv"] = "samples_kv"
	tableNames["samples_kv_dist"] = "samples_kv_dist"
	tableNames["time_series_gin"] = "time_series_gin"
	tableNames["time_series_gin_dist"] = "time_series_gin_dist"
	tableNames["samples_v3"] = "samples_v3"
	tableNames["samples_v3_dist"] = "samples_v3_dist"
	tableNames["metrics_15s"] = "metrics_15s"
	tableNames["profiles_series"] = "profiles_series"
	tableNames["profiles_series_gin"] = "profiles_series_gin"
	tableNames["profiles"] = "profiles"
	tableNames["tempo_traces_attrs_gin"] = "tempo_traces_attrs_gin"
	tableNames["tempo_traces_attrs_gin_dist"] = "tempo_traces_attrs_gin_dist"
	tableNames["patterns"] = "patterns"
}

func GetTableName(name string) string {
	lock.RLock()
	defer lock.RUnlock()
	p := plugins.GetTableNamesPlugin()
	if p == nil {
		return tableNames[name]
	}
	n := (*p)()[name]
	if n == "" {
		return tableNames[name]
	}
	return n
}

func PopulateTableNames(ctx *shared.PlannerContext, db *model.DataDatabasesMap) *shared.PlannerContext {
	ctx.SamplesTableName = GetTableName("samples_v3")
	ctx.SamplesDistTableName = GetTableName("samples_v3")
	ctx.TimeSeriesTableName = GetTableName("time_series")
	ctx.TimeSeriesDistTableName = GetTableName("time_series")
	ctx.TimeSeriesGinTableName = GetTableName("time_series_gin")
	ctx.TimeSeriesGinDistTableName = GetTableName("time_series_gin")
	ctx.Metrics15sTableName = GetTableName("metrics_15s")
	ctx.Metrics15sDistTableName = GetTableName("metrics_15s")

	ctx.ProfilesSeriesGinTable = GetTableName("profiles_series_gin")
	ctx.ProfilesSeriesGinDistTable = GetTableName("profiles_series_gin")
	ctx.ProfilesTable = GetTableName("profiles")
	ctx.ProfilesDistTable = GetTableName("profiles")
	ctx.ProfilesSeriesTable = GetTableName("profiles_series")
	ctx.ProfilesSeriesDistTable = GetTableName("profiles_series")

	ctx.PatternsTable = GetTableName("patterns")
	ctx.PatternsDistTable = GetTableName("patterns")

	ctx.TracesAttrsTable = GetTableName("tempo_traces_attrs_gin")
	ctx.TracesAttrsDistTable = GetTableName("tempo_traces_attrs_gin")
	ctx.TracesTable = GetTableName("tempo_traces")
	ctx.TracesDistTable = GetTableName("tempo_traces")
	ctx.TracesKVTable = GetTableName("tempo_traces_kv")
	ctx.TracesKVDistTable = GetTableName("tempo_traces_kv")

	if db.Config.ClusterName != "" {
		ctx.SamplesDistTableName = fmt.Sprintf("`%s`.%s_dist", db.Config.Name, ctx.SamplesTableName)
		ctx.TimeSeriesDistTableName = fmt.Sprintf("`%s`.%s_dist", db.Config.Name, ctx.TimeSeriesTableName)
		ctx.TimeSeriesGinDistTableName = fmt.Sprintf("`%s`.%s_dist", db.Config.Name, ctx.TimeSeriesGinTableName)
		ctx.Metrics15sDistTableName = fmt.Sprintf("`%s`.%s_dist", db.Config.Name, ctx.Metrics15sTableName)

		ctx.ProfilesSeriesGinDistTable = fmt.Sprintf("`%s`.%s_dist", db.Config.Name, ctx.ProfilesSeriesGinTable)
		ctx.ProfilesDistTable = fmt.Sprintf("`%s`.%s_dist", db.Config.Name, ctx.ProfilesTable)
		ctx.ProfilesSeriesDistTable = fmt.Sprintf("`%s`.%s_dist", db.Config.Name, ctx.ProfilesSeriesTable)

		ctx.PatternsDistTable = fmt.Sprintf("`%s`.%s_dist", db.Config.Name, ctx.PatternsTable)

		ctx.TracesAttrsDistTable = fmt.Sprintf("`%s`.%s_dist", db.Config.Name, ctx.TracesAttrsTable)
		ctx.TracesDistTable = fmt.Sprintf("`%s`.%s_dist", db.Config.Name, ctx.TracesTable)
		ctx.TracesKVDistTable = fmt.Sprintf("`%s`.%s_dist", db.Config.Name, ctx.TracesKVTable)
	}
	return ctx
}
