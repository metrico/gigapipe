package router

import (
	"github.com/gorilla/mux"
	controllerv1 "github.com/metrico/qryn/v4/reader/controller"
	"github.com/metrico/qryn/v4/reader/model"
	"github.com/metrico/qryn/v4/reader/service"
)

func RouteSelectPrometheusLabels(app *mux.Router, dataSession model.IDBRegistry) {
	qrService := service.NewQueryLabelsService(&model.ServiceData{
		Session: dataSession,
	})
	qrCtrl := &controllerv1.PromQueryLabelsController{
		QueryLabelsService: qrService,
	}
	app.HandleFunc("/api/v1/labels", qrCtrl.PromLabels).Methods("GET", "POST", "OPTIONS")
	app.HandleFunc("/api/v1/label/{name}/values", qrCtrl.LabelValues).Methods("GET", "OPTIONS")
	app.HandleFunc("/api/v1/metadata", qrCtrl.Metadata).Methods("GET", "OPTIONS")
	app.HandleFunc("/api/v1/query_exemplars", qrCtrl.Metadata).Methods("GET", "OPTIONS")
	app.HandleFunc("/api/v1/rules", qrCtrl.Metadata).Methods("GET", "OPTIONS")
	app.HandleFunc("/api/v1/series", qrCtrl.Series).Methods("GET", "POST", "OPTIONS")
}
