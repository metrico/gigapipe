package plugins

import (
	"time"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
)

type SeriesPlannerPlugin func(main shared.SQLRequestPlanner) shared.SQLRequestPlanner

var seriesPlannerPlugin *SeriesPlannerPlugin

func RegisterSeriesPlannerPlugin(plugin SeriesPlannerPlugin) {
	seriesPlannerPlugin = &plugin
}

func GetSeriesPlannerPlugin() *SeriesPlannerPlugin {
	return seriesPlannerPlugin
}

type Metrics15ShortcutPlannerPlugin func(fn string, duration time.Duration) shared.SQLRequestPlanner

var metrics15ShortcutPlannerPlugin *Metrics15ShortcutPlannerPlugin

func RegisterMetrics15ShortcutPlannerPlugin(plugin Metrics15ShortcutPlannerPlugin) {
	metrics15ShortcutPlannerPlugin = &plugin
}

func GetMetrics15ShortcutPlannerPlugin() *Metrics15ShortcutPlannerPlugin {
	return metrics15ShortcutPlannerPlugin
}

type TimeSeriesInitPlannerPlugin func() shared.SQLRequestPlanner

var timeSeriesInitPlannerPlugin *TimeSeriesInitPlannerPlugin

func RegisterTimeSeriesInitPlannerPlugin(plugin TimeSeriesInitPlannerPlugin) {
	timeSeriesInitPlannerPlugin = &plugin
}

func GetTimeSeriesInitPlannerPlugin() *TimeSeriesInitPlannerPlugin {
	return timeSeriesInitPlannerPlugin
}

type SqlMainInitPlannerPlugin func() shared.SQLRequestPlanner

var sqlMainInitPlannerPlugin *SqlMainInitPlannerPlugin

func RegisterSqlMainInitPlannerPlugin(plugin SqlMainInitPlannerPlugin) {
	sqlMainInitPlannerPlugin = &plugin
}

func GetSqlMainInitPlannerPlugin() *SqlMainInitPlannerPlugin {
	return sqlMainInitPlannerPlugin
}

type ValuesPlannerPlugin func(main shared.SQLRequestPlanner, key string) shared.SQLRequestPlanner

var valuesPlannerPlugin *ValuesPlannerPlugin

func RegisterValuesPlannerPlugin(plugin ValuesPlannerPlugin) {
	valuesPlannerPlugin = &plugin
}

func GetValuesPlannerPlugin() *ValuesPlannerPlugin {
	return valuesPlannerPlugin
}

type StreamSelectPlannerPlugin func(LabelNames []string, ops []string, values []string) shared.SQLRequestPlanner

var streamSelectPlannerPlugin *StreamSelectPlannerPlugin

func RegisterStreamSelectPlannerPlugin(plugin StreamSelectPlannerPlugin) {
	streamSelectPlannerPlugin = &plugin
}

func GetStreamSelectPlannerPlugin() *StreamSelectPlannerPlugin {
	return streamSelectPlannerPlugin
}
