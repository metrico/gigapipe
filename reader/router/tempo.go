package router

import (
	"github.com/gorilla/mux"
	controllerv1 "github.com/metrico/qryn/v4/reader/controller"
	"github.com/metrico/qryn/v4/reader/model"
	"github.com/metrico/qryn/v4/reader/service"
)

func RouteTempo(app *mux.Router, dataSession model.IDBRegistry) {
	tempoSvc := service.NewTempoService(model.ServiceData{
		Session: dataSession,
	})
	ctrl := &controllerv1.TempoController{
		Controller: controllerv1.Controller{},
		Service:    tempoSvc,
	}
	app.HandleFunc("/tempo/api/traces/{traceId}", ctrl.Trace).Methods("GET", "OPTIONS")
	app.HandleFunc("/api/traces/{traceId}", ctrl.Trace).Methods("GET", "OPTIONS")
	app.HandleFunc("/api/traces/{traceId}/json", ctrl.Trace).Methods("GET", "OPTIONS")
	app.HandleFunc("/tempo/api/echo", ctrl.Echo).Methods("GET", "OPTIONS")
	app.HandleFunc("/api/echo", ctrl.Echo).Methods("GET", "OPTIONS")
	app.HandleFunc("/tempo/api/search/tags", ctrl.Tags).Methods("GET", "OPTIONS")
	app.HandleFunc("/api/search/tags", ctrl.Tags).Methods("GET", "OPTIONS")
	app.HandleFunc("/tempo/api/search/tag/{tag}/values", ctrl.Values).Methods("GET", "OPTIONS")
	app.HandleFunc("/api/search/tag/{tag}/values", ctrl.Values).Methods("GET", "OPTIONS")
	app.HandleFunc("/api/v2/search/tag/{tag}/values", ctrl.ValuesV2).Methods("GET", "OPTIONS")
	app.HandleFunc("/api/v2/search/tags", ctrl.TagsV2).Methods("GET", "OPTIONS")
	app.HandleFunc("/tempo/api/search", ctrl.Search).Methods("GET", "OPTIONS")
	app.HandleFunc("/api/search", ctrl.Search).Methods("GET", "OPTIONS")
}
