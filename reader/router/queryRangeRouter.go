package apirouterv1

import (
	"github.com/gorilla/mux"
	"github.com/metrico/qryn/reader/config"
	controllerv1 "github.com/metrico/qryn/reader/controller"
	"github.com/metrico/qryn/reader/model"
	"github.com/metrico/qryn/reader/service"
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
	app.HandleFunc("/loki/api/v1/query_range", qrCtrl.QueryRange).Methods("GET")
	app.HandleFunc("/loki/api/v1/query", qrCtrl.Query).Methods("GET")
	app.HandleFunc("/loki/api/v1/tail", qrCtrl.Tail).Methods("GET")

	if config.Cloki.Setting.DRILLDOWN_SETTINGS.LogDrilldown {
		vCtrl := &controllerv1.VolumeController{
			QueryRangeService: qrService,
		}
		app.HandleFunc("/loki/api/v1/index/volume", vCtrl.Volume).Methods("GET")
		app.HandleFunc("/loki/api/v1/detected_labels", vCtrl.DetectedLabels).Methods("GET")
		app.HandleFunc("/loki/api/v1/detected_fields", vCtrl.DetectedFields).Methods("GET")
		app.HandleFunc("/loki/api/v1/patterns", vCtrl.Patterns).Methods("GET")
	}
}
