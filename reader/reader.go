package reader

import (
	"fmt"
	"net"
	"net/http"
	"runtime"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/qryn/v5/reader/config"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/registry"
	"github.com/metrico/qryn/v5/reader/router"
	"github.com/metrico/qryn/v5/reader/utils/logger"
	"github.com/metrico/qryn/v5/reader/utils/middleware"
	"github.com/metrico/qryn/v5/reader/watchdog"
)

// Init registers the reader routes on app. With a nil app the reader builds
// its own mux, wraps it in the server middleware chain and serves it on the
// configured host and port; out-of-tree binaries rely on that.
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

	if app != nil {
		performV1APIRouting(app)
		return
	}

	app = http.NewServeMux()
	performV1APIRouting(app)
	httpURL := fmt.Sprintf("%s:%d", config.Cloki.Setting.HTTP_SETTINGS.Host, config.Cloki.Setting.HTTP_SETTINGS.Port)
	httpStart(middleware.WrapServer(config.Cloki, app), httpURL)
}

func Stop() {
	logger.Info("Stopping Reader module...")
	watchdog.Stop()
	logger.Info("Reader watchdog stopped.")
	registry.Stop()
	logger.Info("Reader registry stopped.")
	logger.Info("Reader module stopped.")
}

func httpStart(server http.Handler, httpURL string) {
	logger.Info("Starting service")
	http.Handle("/", server)
	listener, err := net.Listen("tcp", httpURL)
	if err != nil {
		logger.Error("Error creating listener:", err)
		panic(err)
	}
	logger.Info("Server is listening on", httpURL)
	if err := http.Serve(listener, server); err != nil {
		logger.Error("Error serving:", err)
		panic(err)
	}
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
