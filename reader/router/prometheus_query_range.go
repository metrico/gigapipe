package router

import (
	"log/slog"
	"os"
	"time"

	"github.com/gorilla/mux"
	"github.com/metrico/qryn/v5/reader/config"
	controllerv1 "github.com/metrico/qryn/v5/reader/controller"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/promql/promql_transpiler"
	"github.com/metrico/qryn/v5/reader/service"
	"github.com/metrico/qryn/v5/reader/utils/logger"
	"github.com/metrico/qryn/v5/shared/metricretention"
	"github.com/prometheus/prometheus/promql"
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
		LookbackDelta:      promql_transpiler.EngineLookbackDelta,
		// A non-nil function is required: the engine calls it for subqueries
		// that omit a resolution step (e.g. `up[1h:]`). Leaving it nil panics
		// with a nil pointer dereference in getLastSubqueryInterval.
		NoStepSubqueryIntervalFn: func(int64) int64 {
			return defaultSubqueryInterval.Milliseconds()
		},
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
	})
}

func RoutePrometheusQueryRange(app *mux.Router, dataSession model.IDBRegistry,
	stats bool,
) {
	eng := NewPromEngine(config.Cloki.Setting.SYSTEM_SETTINGS.MetricsMaxSamples)
	svc := service.CLokiQueriable{
		Session: dataSession,
		Tiers:   tierRouting(),
	}
	ctrl := &controllerv1.PromQueryRangeController{
		Controller: controllerv1.Controller{},
		Engine:     eng,
		Storage:    &svc,
		Stats:      stats,
	}
	app.HandleFunc("/api/v1/query_range", ctrl.QueryRange).Methods("GET", "POST", "OPTIONS")
	app.HandleFunc("/api/v1/query", ctrl.QueryInstant).Methods("GET", "POST", "OPTIONS")
}

// tierRouting reads the tier lifetimes and METRICS_READ_TIER. The raw tier's lifetime defaults
// to the first database's ttl_days, 7 when unset. Without valid lifetimes every read is raw.
func tierRouting() *service.TierRouting {
	samplesDays := 7
	if dbs := config.Cloki.Setting.DATABASE_DATA; len(dbs) > 0 && dbs[0].TTLDays > 0 {
		samplesDays = dbs[0].TTLDays
	}
	lifetimes, err := metricretention.FromEnv(samplesDays, os.Getenv)
	if err != nil {
		logger.Error("metric tiers: ", err.Error())
		return nil
	}
	return &service.TierRouting{Lifetimes: lifetimes, Forced: os.Getenv("METRICS_READ_TIER")}
}
