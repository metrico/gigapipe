package router

import (
	"log/slog"
	"time"

	"github.com/gorilla/mux"
	grafana_re "github.com/grafana/regexp"
	"github.com/metrico/qryn/v4/reader/config"
	controllerv1 "github.com/metrico/qryn/v4/reader/controller"
	"github.com/metrico/qryn/v4/reader/model"
	"github.com/metrico/qryn/v4/reader/service"
	"github.com/metrico/qryn/v4/reader/utils/logger"
	"github.com/prometheus/prometheus/promql"
	api_v1 "github.com/prometheus/prometheus/web/api/v1"
)

// defaultSubqueryInterval is used as the resolution step for subqueries that
// omit one (e.g. `up[1h:]`). It matches Prometheus' default evaluation interval.
const defaultSubqueryInterval = time.Minute

// NewPromEngine builds the PromQL engine used to serve /api/v1/query and
// /api/v1/query_range requests.
func NewPromEngine(maxSamples int) *promql.Engine {
	slogLogger := slog.New(slog.NewJSONHandler(logger.Logger.Out, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	return promql.NewEngine(promql.EngineOpts{
		Logger:             slogLogger,
		Reg:                nil,
		MaxSamples:         maxSamples,
		Timeout:            time.Second * 30,
		ActiveQueryTracker: nil,
		LookbackDelta:      0,
		// A non-nil function is required: the engine calls it for subqueries
		// that omit a resolution step (e.g. `up[1h:]`). Leaving it nil panics
		// with a nil pointer dereference in getLastSubqueryInterval.
		NoStepSubqueryIntervalFn: func(int64) int64 {
			return defaultSubqueryInterval.Milliseconds()
		},
		EnableAtModifier:     true,
		EnableNegativeOffset: false,
	})
}

func RoutePrometheusQueryRange(app *mux.Router, dataSession model.IDBRegistry,
	stats bool,
) {
	eng := NewPromEngine(config.Cloki.Setting.SYSTEM_SETTINGS.MetricsMaxSamples)
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
