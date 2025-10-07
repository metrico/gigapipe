package router

import (
	"time"

	kitlog "github.com/go-kit/kit/log/logrus"
	"github.com/gorilla/mux"
	grafana_re "github.com/grafana/regexp"
	"github.com/metrico/qryn/reader/config"
	controllerv1 "github.com/metrico/qryn/reader/controller"
	"github.com/metrico/qryn/reader/model"
	"github.com/metrico/qryn/reader/service"
	"github.com/metrico/qryn/reader/utils/logger"
	"github.com/prometheus/prometheus/promql"
	api_v1 "github.com/prometheus/prometheus/web/api/v1"
)

func RoutePrometheusQueryRange(app *mux.Router, dataSession model.IDBRegistry,
	stats bool,
) {
	eng := promql.NewEngine(promql.EngineOpts{
		Logger:                   kitlog.NewLogger(logger.Logger),
		Reg:                      nil,
		MaxSamples:               config.Cloki.Setting.SYSTEM_SETTINGS.MetricsMaxSamples,
		Timeout:                  time.Second * 30,
		ActiveQueryTracker:       nil,
		LookbackDelta:            0,
		NoStepSubqueryIntervalFn: nil,
		EnableAtModifier:         false,
		EnableNegativeOffset:     false,
	})
	svc := service.CLokiQueriable{
		ServiceData: model.ServiceData{Session: dataSession},
	}
	api := api_v1.API{
		Queryable:         nil,
		QueryEngine:       eng,
		ExemplarQueryable: nil,
		CORSOrigin:        grafana_re.MustCompile("\\*"),
	}
	ctrl := &controllerv1.PromQueryRangeController{
		Controller: controllerv1.Controller{},
		Api:        &api,
		Storage:    &svc,
		Stats:      stats,
	}
	app.HandleFunc("/api/v1/query_range", ctrl.QueryRange).Methods("GET", "POST", "OPTIONS")
	app.HandleFunc("/api/v1/query", ctrl.QueryInstant).Methods("GET", "POST", "OPTIONS")
}
