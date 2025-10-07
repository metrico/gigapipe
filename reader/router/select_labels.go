package router

import (
	"github.com/gorilla/mux"
	controllerv1 "github.com/metrico/qryn/reader/controller"
	"github.com/metrico/qryn/reader/model"
	"github.com/metrico/qryn/reader/service"
)

func RouteSelectLabels(app *mux.Router, dataSession model.IDBRegistry) {
	qrService := service.NewQueryLabelsService(&model.ServiceData{
		Session: dataSession,
	})
	qrCtrl := &controllerv1.QueryLabelsController{
		QueryLabelsService: qrService,
	}
	app.HandleFunc("/loki/api/v1/label", qrCtrl.Labels).Methods("GET", "POST", "OPTIONS")
	app.HandleFunc("/loki/api/v1/labels", qrCtrl.Labels).Methods("GET", "POST", "OPTIONS")
	app.HandleFunc("/loki/api/v1/label/{name}/values", qrCtrl.Values).Methods("GET", "POST", "OPTIONS")
	app.HandleFunc("/loki/api/v1/series", qrCtrl.Series).Methods("GET", "POST", "OPTIONS")
}
