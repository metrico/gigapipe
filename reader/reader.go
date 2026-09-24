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
	httpStart(applyMiddlewares(app), httpURL)
}

// applyMiddlewares wraps the standalone mux in the same chain as
// cmd/gigapipe/main.go, outermost first: Logging, Cors, AcceptEncoding,
// BasicAuth. The gzip writer depends on the logging writer's Flush/Unwrap
// sitting between it and the transport.
func applyMiddlewares(app *http.ServeMux) http.Handler {
	var handler http.Handler = app
	if config.Cloki.Setting.AUTH_SETTINGS.BASIC.Username != "" &&
		config.Cloki.Setting.AUTH_SETTINGS.BASIC.Password != "" {
		handler = middleware.BasicAuthMiddleware(config.Cloki.Setting.AUTH_SETTINGS.BASIC.Username,
			config.Cloki.Setting.AUTH_SETTINGS.BASIC.Password)(handler)
	}
	handler = middleware.AcceptEncodingMiddleware(handler)
	if config.Cloki.Setting.HTTP_SETTINGS.Cors.Enable {
		handler = middleware.CorsMiddleware(config.Cloki.Setting.HTTP_SETTINGS.Cors.Origin)(handler)
	}
	return middleware.LoggingMiddleware("[{{.status}}] {{.method}} {{.url}} - LAT:{{.latency}}")(handler)
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
