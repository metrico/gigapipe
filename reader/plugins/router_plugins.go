package plugins

import (
	"github.com/gorilla/mux"
	"github.com/metrico/qryn/v4/reader/model"
	"github.com/prometheus/prometheus/storage"
)

type Services struct {
	TempoService       model.ITempoService
	QueryLabelsService model.IQueryLabelsService
	PrometheusService  storage.Queryable
	QueryRangeService  model.IQueryRangeService
	ServiceData        model.ServiceData
}

type IRoutePlugin interface {
	Route(router *mux.Router)
	SetServices(services Services)
}

var routePlugins []IRoutePlugin

func RegisterRoutePlugin(name string, p IRoutePlugin) {
	routePlugins = append(routePlugins, p)
}

func GetRoutePlugins() []IRoutePlugin {
	return routePlugins
}
