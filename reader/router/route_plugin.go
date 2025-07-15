package router

import (
	"github.com/gorilla/mux"
	"github.com/metrico/qryn/reader/model"
	"github.com/metrico/qryn/reader/plugins"
	"github.com/metrico/qryn/reader/service"
)

func PluggableRoutes(app *mux.Router, dataSession model.IDBRegistry) {
	sd := model.ServiceData{
		Session: dataSession,
	}
	services := plugins.Services{
		TempoService:       service.NewTempoService(sd),
		QueryLabelsService: service.NewQueryLabelsService(&sd),
		PrometheusService:  &service.CLokiQueriable{ServiceData: sd},
		QueryRangeService:  &service.QueryRangeService{ServiceData: sd},
		ServiceData:        sd,
	}
	for _, r := range plugins.GetRoutePlugins() {
		r.SetServices(services)
		r.Route(app)
	}
}
