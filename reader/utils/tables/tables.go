package tables

import (
	"fmt"
	"sync"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/plugins"
	"github.com/metrico/qryn/v5/shared/distconfig"
	"github.com/metrico/qryn/v5/shared/samplesconfig"
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
	tableNames["samples_logs"] = "samples_logs"
	tableNames["samples_logs_dist"] = "samples_logs_dist"
	tableNames["samples_metrics"] = "samples_metrics"
	tableNames["samples_metrics_dist"] = "samples_metrics_dist"
	tableNames["logs_aggr"] = "logs_aggr"
	tableNames["metrics_aggr"] = "metrics_aggr"
}

// InitDistTableNames re-registers dist table names using the configured suffix.
// Must be called after distconfig.Init().
func InitDistTableNames() {
	lock.Lock()
	defer lock.Unlock()
	suffix := distconfig.Suffix()
	tableNames["tempo_traces_dist"] = "tempo_traces" + suffix
	tableNames["tempo_traces_kv_dist"] = "tempo_traces_kv" + suffix
	tableNames["time_series_dist"] = "time_series" + suffix
	tableNames["samples_kv_dist"] = "samples_kv" + suffix
	tableNames["time_series_gin_dist"] = "time_series_gin" + suffix
	tableNames["samples_v3_dist"] = "samples_v3" + suffix
	tableNames["tempo_traces_attrs_gin_dist"] = "tempo_traces_attrs_gin" + suffix
	tableNames["samples_logs_dist"] = "samples_logs" + suffix
	tableNames["samples_metrics_dist"] = "samples_metrics" + suffix
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
	// Type is the signal selector every reader entry point already sets: PromQL
	// and the prometheus label routes pass 2, the loki ones pass 1 or 0.
	metrics := ctx.Type == shared.SAMPLES_TYPE_METRICS
	ctx.SamplesTableName = GetTableName(samplesconfig.SamplesTable(metrics))
	ctx.SamplesDistTableName = ctx.SamplesTableName
	ctx.TimeSeriesTableName = GetTableName("time_series")
	ctx.TimeSeriesDistTableName = GetTableName("time_series")
	ctx.TimeSeriesGinTableName = GetTableName("time_series_gin")
	ctx.TimeSeriesGinDistTableName = GetTableName("time_series_gin")
	ctx.Metrics15sTableName = GetTableName(samplesconfig.AggrTable(metrics))
	ctx.Metrics15sDistTableName = ctx.Metrics15sTableName
	ctx.AggrInterval = aggrInterval(metrics)

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
		suffix := distconfig.Suffix()
		ctx.SamplesDistTableName = fmt.Sprintf("`%s`.%s%s", db.Config.Name, ctx.SamplesTableName, suffix)
		ctx.TimeSeriesDistTableName = fmt.Sprintf("`%s`.%s%s", db.Config.Name, ctx.TimeSeriesTableName, suffix)
		ctx.TimeSeriesGinDistTableName = fmt.Sprintf("`%s`.%s%s", db.Config.Name, ctx.TimeSeriesGinTableName, suffix)
		ctx.Metrics15sDistTableName = fmt.Sprintf("`%s`.%s%s", db.Config.Name, ctx.Metrics15sTableName, suffix)

		ctx.ProfilesSeriesGinDistTable = fmt.Sprintf("`%s`.%s%s", db.Config.Name, ctx.ProfilesSeriesGinTable, suffix)
		ctx.ProfilesDistTable = fmt.Sprintf("`%s`.%s%s", db.Config.Name, ctx.ProfilesTable, suffix)
		ctx.ProfilesSeriesDistTable = fmt.Sprintf("`%s`.%s%s", db.Config.Name, ctx.ProfilesSeriesTable, suffix)

		ctx.PatternsDistTable = fmt.Sprintf("`%s`.%s%s", db.Config.Name, ctx.PatternsTable, suffix)

		ctx.TracesAttrsDistTable = fmt.Sprintf("`%s`.%s%s", db.Config.Name, ctx.TracesAttrsTable, suffix)
		ctx.TracesDistTable = fmt.Sprintf("`%s`.%s%s", db.Config.Name, ctx.TracesTable, suffix)
		ctx.TracesKVDistTable = fmt.Sprintf("`%s`.%s%s", db.Config.Name, ctx.TracesKVTable, suffix)
	}
	return ctx
}

// aggrInterval is the bucket width of the preaggregate one signal reads, or 0
// when it has none. Logs always have one, fixed at 15s. Metrics have one only
// while the aggregation switch holds -- and without the split that switch does
// not apply, since the shared metrics_15s view is always there.
func aggrInterval(metrics bool) time.Duration {
	if metrics && samplesconfig.SplitBySignal() && !samplesconfig.MetricsAggrEnabled() {
		return 0
	}
	if metrics && samplesconfig.SplitBySignal() {
		return samplesconfig.MetricsAggrInterval()
	}
	return samplesconfig.LogsAggrInterval()
}
