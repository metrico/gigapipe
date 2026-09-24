package reader

import (
	"net/http"
	"runtime"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/qryn/v5/reader/config"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/registry"
	"github.com/metrico/qryn/v5/reader/router"
	"github.com/metrico/qryn/v5/reader/utils/logger"
	"github.com/metrico/qryn/v5/reader/watchdog"
)

func Init(cnf *clconfig.ClokiConfig, app *http.ServeMux) {
	config.Cloki = cnf

	// Set to max cpu if the value is equals 0
	if config.Cloki.Setting.SYSTEM_SETTINGS.CPUMaxProcs == 0 {
		runtime.GOMAXPROCS(runtime.NumCPU())
	} else {
		runtime.GOMAXPROCS(config.Cloki.Setting.SYSTEM_SETTINGS.CPUMaxProcs)
	}

	// initialize logger
	//
	logger.InitLogger()

	performV1APIRouting(app)
}

func Stop() {
	logger.Info("Stopping Reader module...")
	watchdog.Stop()
	logger.Info("Reader watchdog stopped.")
	registry.Stop()
	logger.Info("Reader registry stopped.")
	logger.Info("Reader module stopped.")
}

func performV1APIRouting(acc *http.ServeMux) {
	registry.Init()
	watchdog.Init(&model.ServiceData{Session: registry.Registry})

	router.RouteQueryRangeApis(acc, registry.Registry)
	router.RouteSelectLabels(acc, registry.Registry)
	router.RouteSelectPrometheusLabels(acc, registry.Registry)
	router.RoutePrometheusQueryRange(acc, registry.Registry, config.Cloki.Setting.SYSTEM_SETTINGS.QueryStats)
	router.RouteTempo(acc, registry.Registry)
	router.RouteMiscApis(acc)
	router.RouteProf(acc, registry.Registry)
	router.PluggableRoutes(acc, registry.Registry)
}
