package router

import (
	"github.com/gorilla/mux"
	controllerv1 "github.com/metrico/qryn/v5/reader/controller"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/service"
)

func RouteSelectPrometheusLabels(app *mux.Router, dataSession model.IDBRegistry) {
	ctrl := &controllerv1.PromQueryLabelsController{
		MetricLabelsService: service.NewMetricLabelsService(&model.ServiceData{Session: dataSession}),
	}
	app.HandleFunc("/api/v1/labels", ctrl.PromLabels).Methods("GET", "POST", "OPTIONS")
	app.HandleFunc("/api/v1/label/{name}/values", ctrl.LabelValues).Methods("GET", "OPTIONS")
	app.HandleFunc("/api/v1/series", ctrl.Series).Methods("GET", "POST", "OPTIONS")
	app.HandleFunc("/api/v1/metadata", ctrl.Metadata).Methods("GET", "OPTIONS")
	app.HandleFunc("/api/v1/query_exemplars", ctrl.QueryExemplars).Methods("GET", "POST", "OPTIONS")
	// /api/v1/rules is owned by the ruler module (recording rules), which
	// registers it when enabled.
}
