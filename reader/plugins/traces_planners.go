package plugins

import (
	"context"
	"time"

	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/reader/model"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type TracesDataPlugin func(main shared.SQLRequestPlanner) shared.SQLRequestPlanner

var traceDataPlugin *TracesDataPlugin

func RegisterTracesDataPlugin(plugin TracesDataPlugin) {
	traceDataPlugin = &plugin
}

func GetTracesDataPlugin() *TracesDataPlugin {
	return traceDataPlugin
}

type AttrlessConditionPlannerPlugin func() shared.SQLRequestPlanner

var attrlessConditionPlannerPlugin *AttrlessConditionPlannerPlugin

func RegisterAttrlessConditionPlannerPlugin(plugin AttrlessConditionPlannerPlugin) {
	attrlessConditionPlannerPlugin = &plugin
}

func GetAttrlessConditionPlannerPlugin() *AttrlessConditionPlannerPlugin {
	return attrlessConditionPlannerPlugin
}

type GetTracesQueryPlugin func(ctx context.Context, idx any, limit int, fromNS int64, toNS int64,
	distributed bool, minDurationNS int64, maxDurationNS int64) (sql.ISelect, error)

var getTracesQueryPlugin *GetTracesQueryPlugin

func RegisterGetTracesQueryPlugin(plugin GetTracesQueryPlugin) {
	getTracesQueryPlugin = &plugin
}

func GetGetTracesQueryPlugin() *GetTracesQueryPlugin {
	return getTracesQueryPlugin
}

type LabelsGetterPlugin interface {
	GetLabelsQuery(ctx context.Context, conn *model.DataDatabasesMap,
		fingerprints map[uint64]bool, from time.Time, to time.Time) sql.ISelect
}

var labelsGetterPlugin *LabelsGetterPlugin

func RegisterLabelsGetterPlugin(plugin LabelsGetterPlugin) {
	labelsGetterPlugin = &plugin
}

func GetLabelsGetterPlugin() *LabelsGetterPlugin {
	return labelsGetterPlugin
}

type InitIndexPlannerPlugin func() shared.SQLRequestPlanner

var initIndexPlannerPlugin *InitIndexPlannerPlugin

func RegisterInitIndexPlannerPlugin(plugin InitIndexPlannerPlugin) {
	initIndexPlannerPlugin = &plugin
}

func GetInitIndexPlannerPlugin() *InitIndexPlannerPlugin {
	return initIndexPlannerPlugin
}
