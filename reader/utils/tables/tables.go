package tables

import (
	"fmt"
	"sync"

	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/reader/model"
	"github.com/metrico/qryn/reader/plugins"
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
	tsGinTable := GetTableName("time_series_gin")
	samplesTableName := GetTableName("samples_v3")
	timeSeriesTableName := GetTableName("time_series")
	timeSeriesDistTableName := GetTableName("time_series")
	patternsTableName := GetTableName("patterns")

	ctx.ProfilesSeriesGinTable = GetTableName("profiles_series_gin")
	ctx.ProfilesSeriesGinDistTable = GetTableName("profiles_series_gin")
	ctx.ProfilesTable = GetTableName("profiles")
	ctx.ProfilesDistTable = GetTableName("profiles")
	ctx.ProfilesSeriesTable = GetTableName("profiles_series")
	ctx.ProfilesSeriesDistTable = GetTableName("profiles_series")
	ctx.PatternsTable = patternsTableName
	ctx.PatternsDistTable = patternsTableName
	ctx.Metrics15sTableName = GetTableName("metrics_15s")
	ctx.Metrics15sDistTableName = GetTableName("metrics_15s")

	ctx.TracesAttrsTable = GetTableName("tempo_traces_attrs_gin")
	ctx.TracesAttrsDistTable = GetTableName("tempo_traces_attrs_gin")
	ctx.TracesTable = GetTableName("tempo_traces")
	ctx.TracesDistTable = GetTableName("tempo_traces")
	ctx.TracesKVTable = GetTableName("tempo_traces_kv")
	ctx.TracesKVDistTable = GetTableName("tempo_traces_kv")

	if db.Config.ClusterName != "" {
		tsGinTable = fmt.Sprintf("`%s`.%s", db.Config.Name, tsGinTable)
		samplesTableName = fmt.Sprintf("`%s`.%s_dist", db.Config.Name, samplesTableName)
		timeSeriesTableName = fmt.Sprintf("`%s`.%s", db.Config.Name, timeSeriesTableName)
		timeSeriesDistTableName = fmt.Sprintf("`%s`.%s_dist", db.Config.Name, timeSeriesDistTableName)
		ctx.ProfilesSeriesGinDistTable = fmt.Sprintf("`%s`.%s_dist", db.Config.Name, ctx.ProfilesSeriesGinTable)
		ctx.ProfilesDistTable = fmt.Sprintf("`%s`.%s_dist", db.Config.Name, ctx.ProfilesTable)
		ctx.ProfilesSeriesDistTable = fmt.Sprintf("`%s`.%s_dist", db.Config.Name, ctx.ProfilesSeriesTable)
		ctx.TracesAttrsDistTable = fmt.Sprintf("`%s`.%s_dist", db.Config.Name, ctx.TracesAttrsTable)
		ctx.TracesDistTable = fmt.Sprintf("`%s`.%s_dist", db.Config.Name, ctx.TracesTable)
		ctx.TracesKVDistTable = fmt.Sprintf("`%s`.%s_dist", db.Config.Name, ctx.TracesKVTable)
		ctx.PatternsDistTable = fmt.Sprintf("`%s`.%s_dist", db.Config.Name, ctx.PatternsTable)
		ctx.Metrics15sDistTableName = fmt.Sprintf("`%s`.%s_dist", db.Config.Name, ctx.Metrics15sTableName)
	}
	ctx.TimeSeriesGinTableName = tsGinTable
	ctx.SamplesTableName = samplesTableName
	ctx.TimeSeriesTableName = timeSeriesTableName
	ctx.TimeSeriesDistTableName = timeSeriesDistTableName
	return ctx
}
