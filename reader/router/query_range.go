package router

import (
	"net/http"

	"github.com/metrico/qryn/v5/reader/config"
	controllerv1 "github.com/metrico/qryn/v5/reader/controller"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/service"
)

func RouteQueryRangeApis(app *http.ServeMux, dataSession model.IDBRegistry) {
	qrService := &service.QueryRangeService{
		Session: dataSession,
	}
	qrCtrl := &controllerv1.QueryRangeController{
		QueryRangeService: qrService,
	}
	app.HandleFunc("GET /loki/api/v1/query_range", qrCtrl.QueryRange)
	app.HandleFunc("GET /loki/api/v1/query", qrCtrl.Query)
	app.HandleFunc("GET /loki/api/v1/tail", qrCtrl.Tail)
	app.HandleFunc("GET /loki/api/v1/index/stats", qrCtrl.IndexStats)

	if config.Cloki.Setting.DRILLDOWN_SETTINGS.LogDrilldown {
		vCtrl := &controllerv1.VolumeController{
			QueryRangeService: qrService,
		}
		app.HandleFunc("GET /loki/api/v1/index/volume", vCtrl.Volume)
		app.HandleFunc("GET /loki/api/v1/detected_labels", vCtrl.DetectedLabels)
		app.HandleFunc("GET /loki/api/v1/detected_fields", vCtrl.DetectedFields)
		app.HandleFunc("GET /loki/api/v1/patterns", vCtrl.Patterns)
	}
}
