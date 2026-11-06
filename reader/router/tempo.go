package router

import (
	"net/http"

	controllerv1 "github.com/metrico/qryn/v5/reader/controller"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/service"
)

func RouteTempo(app *http.ServeMux, dataSession model.IDBRegistry) {
	tempoSvc := service.NewTempoService(model.ServiceData{
		Session: dataSession,
	})
	ctrl := &controllerv1.TempoController{
		Controller: controllerv1.Controller{},
		Service:    tempoSvc,
	}
	app.HandleFunc("GET /tempo/api/traces/{traceId}", ctrl.Trace)
	app.HandleFunc("GET /api/traces/{traceId}", ctrl.Trace)
	app.HandleFunc("GET /api/traces/{traceId}/json", ctrl.Trace)
	app.HandleFunc("GET /tempo/api/echo", ctrl.Echo)
	app.HandleFunc("GET /api/echo", ctrl.Echo)
	app.HandleFunc("GET /tempo/api/search/tags", ctrl.Tags)
	app.HandleFunc("GET /api/search/tags", ctrl.Tags)
	app.HandleFunc("GET /tempo/api/search/tag/{tag}/values", ctrl.Values)
	app.HandleFunc("GET /api/search/tag/{tag}/values", ctrl.Values)
	app.HandleFunc("GET /api/v2/search/tag/{tag}/values", ctrl.ValuesV2)
	app.HandleFunc("GET /api/v2/search/tags", ctrl.TagsV2)
	app.HandleFunc("GET /tempo/api/search", ctrl.Search)
	app.HandleFunc("GET /api/search", ctrl.Search)
	app.HandleFunc("GET /tempo/api/metrics/query_range", ctrl.MetricsQueryRange)
	app.HandleFunc("GET /api/metrics/query_range", ctrl.MetricsQueryRange)
	app.HandleFunc("GET /tempo/api/metrics/query", ctrl.MetricsQueryInstant)
	app.HandleFunc("GET /api/metrics/query", ctrl.MetricsQueryInstant)
}
