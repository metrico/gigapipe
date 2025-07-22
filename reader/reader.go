package reader

import (
	"fmt"
	"net"
	"net/http"
	"runtime"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/gorilla/mux"
	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/qryn/reader/config"
	"github.com/metrico/qryn/reader/model"
	"github.com/metrico/qryn/reader/registry"
	"github.com/metrico/qryn/reader/router"
	"github.com/metrico/qryn/reader/utils/logger"
	"github.com/metrico/qryn/reader/utils/middleware"
	"github.com/metrico/qryn/reader/watchdog"
)

var ownHttpServer bool = false

func Init(cnf *clconfig.ClokiConfig, app *mux.Router) {
	config.Cloki = cnf

	//Set to max cpu if the value is equals 0
	if config.Cloki.Setting.SYSTEM_SETTINGS.CPUMaxProcs == 0 {
		runtime.GOMAXPROCS(runtime.NumCPU())
	} else {
		runtime.GOMAXPROCS(config.Cloki.Setting.SYSTEM_SETTINGS.CPUMaxProcs)
	}

	// initialize logger
	//
	logger.InitLogger()

	if app == nil {
		app = mux.NewRouter()
		ownHttpServer = true
	}

	//Api
	// configure to serve WebServices
	configureAsHTTPServer(app)
}

func configureAsHTTPServer(acc *mux.Router) {
	httpURL := fmt.Sprintf("%s:%d", config.Cloki.Setting.HTTP_SETTINGS.Host, config.Cloki.Setting.HTTP_SETTINGS.Port)
	applyMiddlewares(acc)

	performV1APIRouting(acc)

	if ownHttpServer {
		httpStart(acc, httpURL)
	}
}

func applyMiddlewares(acc *mux.Router) {
	if !ownHttpServer {
		return
	}
	if config.Cloki.Setting.AUTH_SETTINGS.BASIC.Username != "" &&
		config.Cloki.Setting.AUTH_SETTINGS.BASIC.Password != "" {
		acc.Use(middleware.BasicAuthMiddleware(config.Cloki.Setting.AUTH_SETTINGS.BASIC.Username,
			config.Cloki.Setting.AUTH_SETTINGS.BASIC.Password))
	}
	acc.Use(middleware.AcceptEncodingMiddleware)
	if config.Cloki.Setting.HTTP_SETTINGS.Cors.Enable {
		acc.Use(middleware.CorsMiddleware(config.Cloki.Setting.HTTP_SETTINGS.Cors.Origin))
	}
	acc.Use(middleware.LoggingMiddleware("[{{.status}}] {{.method}} {{.url}} - LAT:{{.latency}}"))
}

func httpStart(server *mux.Router, httpURL string) {
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

func performV1APIRouting(acc *mux.Router) {
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
