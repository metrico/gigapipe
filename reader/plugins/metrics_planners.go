package plugins

import "github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"

type InitClickhousePlannerPlugin func() shared.SQLRequestPlanner

var initClickhousePlannerPlugin *InitClickhousePlannerPlugin

func RegisterInitClickhousePlannerPlugin(plugin InitClickhousePlannerPlugin) {
	initClickhousePlannerPlugin = &plugin
}

func GetInitClickhousePlannerPlugin() *InitClickhousePlannerPlugin {
	return initClickhousePlannerPlugin
}

type InitDownsamplePlannerPlugin func() shared.SQLRequestPlanner

var initDownsamplePlannerPlugin *InitDownsamplePlannerPlugin

func RegisterInitDownsamplePlannerPlugin(plugin InitDownsamplePlannerPlugin) {
	initDownsamplePlannerPlugin = &plugin
}

func GetInitDownsamplePlannerPlugin() *InitDownsamplePlannerPlugin {
	return initDownsamplePlannerPlugin
}
