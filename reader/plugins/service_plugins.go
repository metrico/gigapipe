package plugins

import (
	"context"

	"github.com/metrico/qryn/reader/model"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type TempoServicePlugin interface {
	GetQueryRequest(ctx context.Context, startNS int64, endNS int64, traceId []byte,
		conn *model.DataDatabasesMap) sql.ISelect
	GetTagsQuery(ctx context.Context, conn *model.DataDatabasesMap) sql.ISelect
	GetValuesQuery(ctx context.Context, tag string, conn *model.DataDatabasesMap) sql.ISelect
}

var tempoServicePlugin *TempoServicePlugin

func RegisterTempoServicePlugin(factory TempoServicePlugin) {
	tempoServicePlugin = &factory
}

func GetTempoServicePlugin() *TempoServicePlugin {
	return tempoServicePlugin
}

type QueryLabelsServicePlugin interface {
	SetServiceData(data *model.ServiceData)
	EstimateKVComplexity(ctx context.Context, conn *model.DataDatabasesMap) sql.ISelect
	Labels(ctx context.Context, startMs int64, endMs int64, labelsType uint16) (chan string, error)
}

var queryLabelsServicePlugin *QueryLabelsServicePlugin

func RegisterQueryLabelsServicePlugin(plugin QueryLabelsServicePlugin) {
	queryLabelsServicePlugin = &plugin
}

func GetQueryLabelsServicePlugin() *QueryLabelsServicePlugin {
	return queryLabelsServicePlugin
}

type QueryRangeServicePlugin interface {
	SetServiceData(data *model.ServiceData)
	Tail(ctx context.Context, query string) (model.IWatcher, error)
}

var queryRangeServicePlugin *QueryRangeServicePlugin

func RegisterQueryRangeServicePlugin(factory QueryRangeServicePlugin) {
	queryRangeServicePlugin = &factory
}

func GetQueryRangeServicePlugin() *QueryRangeServicePlugin {
	return queryRangeServicePlugin
}
