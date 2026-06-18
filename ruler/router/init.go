package router

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/gorilla/mux"
	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/qryn/v4/ruler"
	"github.com/metrico/qryn/v4/ruler/controller"
	readermodel "github.com/metrico/qryn/v4/reader/model"
	readerregistry "github.com/metrico/qryn/v4/reader/registry"
	readerservice "github.com/metrico/qryn/v4/reader/service"
	readerlogger "github.com/metrico/qryn/v4/reader/utils/logger"
	writercontroller "github.com/metrico/qryn/v4/writer/controller"
	"github.com/prometheus/prometheus/promql"
)

const defaultPollInterval = 30 * time.Second

// managers holds the started rule managers so Stop can shut them down.
var managers []*ruler.RuleManager

// Enabled reports whether the ruler is switched on via QRYN_RULER_ENABLED.
func Enabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("QRYN_RULER_ENABLED"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func pollInterval() time.Duration {
	if v := os.Getenv("QRYN_RULER_POLL_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return defaultPollInterval
}

// Init wires the ruler into the unified binary: it builds the Loki and
// Prometheus rule stores, evaluators, managers and HTTP routes, then starts the
// managers. It is a no-op unless QRYN_RULER_ENABLED is set.
//
// It must be called after writer.Init and reader.Init so the writer's insert
// registry / fingerprint cache / ClickHouse client and the reader's registry
// are all ready.
func Init(cfg *clconfig.ClokiConfig, app *mux.Router) {
	if !Enabled() {
		return
	}
	if writercontroller.DbClient == nil {
		readerlogger.Error("Ruler enabled but writer ClickHouse client is not initialized; ruler not started")
		return
	}

	distributed := len(cfg.Setting.DATABASE_DATA) > 0 && cfg.Setting.DATABASE_DATA[0].ClusterName != ""
	getClient := func() ruler.IChClient { return writercontroller.DbClient }
	writeBack := ruler.NewInProcessWriter()
	poll := pollInterval()

	lokiService := ruler.NewRulerService(getClient, distributed, "loki")
	promService := ruler.NewRulerService(getClient, distributed, "prom")

	session := readerregistry.Registry

	// Loki rule set — evaluated via the reader's instant LogQL service.
	lokiQRS := readerservice.NewQueryRangeService(&readermodel.ServiceData{Session: session})
	lokiMgr := ruler.NewRuleManager(ruler.NewLogQLEvaluator(lokiQRS), lokiService, writeBack, poll)

	// Prometheus rule set — evaluated via the reader's PromQL engine + storage.
	eng := promql.NewEngine(promql.EngineOpts{
		Logger:     slog.New(slog.NewJSONHandler(readerlogger.Logger.Out, &slog.HandlerOptions{Level: slog.LevelWarn})),
		MaxSamples: cfg.Setting.SYSTEM_SETTINGS.MetricsMaxSamples,
		Timeout:    30 * time.Second,
	})
	promStorage := &readerservice.CLokiQueriable{ServiceData: readermodel.ServiceData{Session: session}}
	promMgr := ruler.NewRuleManager(ruler.NewPromEvaluator(eng, promStorage), promService, writeBack, poll)

	lokiCtrl := &controller.Controller{Store: lokiService, Manager: lokiMgr}
	promCtrl := &controller.Controller{Store: promService, Manager: promMgr}
	Route(app, lokiCtrl, promCtrl)

	ctx := context.Background()
	managers = []*ruler.RuleManager{lokiMgr, promMgr}
	for _, m := range managers {
		if err := m.Start(ctx); err != nil {
			readerlogger.Error("Ruler: failed to start manager: ", err.Error())
		}
	}
	readerlogger.Info("Ruler started (poll interval ", poll.String(), ", distributed=", distributed, ")")
}

// Stop shuts down all started rule managers.
func Stop() {
	for _, m := range managers {
		if err := m.Stop(); err != nil {
			readerlogger.Error("Ruler: failed to stop manager: ", err.Error())
		}
	}
	managers = nil
}
