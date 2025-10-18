package router

import (
	"github.com/gorilla/mux"
	"github.com/metrico/qryn/v4/reader/config"
	controllerv1 "github.com/metrico/qryn/v4/reader/controller"
	"github.com/metrico/qryn/v4/reader/model"
	"github.com/metrico/qryn/v4/reader/service"
)

func RouteQueryRangeApis(app *mux.Router, dataSession model.IDBRegistry) {
	qrService := &service.QueryRangeService{
		ServiceData: model.ServiceData{
			Session: dataSession,
		},
	}
	qrCtrl := &controllerv1.QueryRangeController{
		QueryRangeService: qrService,
	}
	app.HandleFunc("/loki/api/v1/query_range", qrCtrl.QueryRange).Methods("GET", "OPTIONS")
	app.HandleFunc("/loki/api/v1/query", qrCtrl.Query).Methods("GET", "OPTIONS")
	app.HandleFunc("/loki/api/v1/tail", qrCtrl.Tail).Methods("GET", "OPTIONS")

	if config.Cloki.Setting.DRILLDOWN_SETTINGS.LogDrilldown {
		vCtrl := &controllerv1.VolumeController{
			QueryRangeService: qrService,
		}
		app.HandleFunc("/loki/api/v1/index/volume", vCtrl.Volume).Methods("GET", "OPTIONS")
		app.HandleFunc("/loki/api/v1/detected_labels", vCtrl.DetectedLabels).Methods("GET", "OPTIONS")
		app.HandleFunc("/loki/api/v1/detected_fields", vCtrl.DetectedFields).Methods("GET", "OPTIONS")
		app.HandleFunc("/loki/api/v1/patterns", vCtrl.Patterns).Methods("GET", "OPTIONS")
	}
}
