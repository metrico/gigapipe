package writer

import (
	"github.com/gorilla/mux"
	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/qryn/v4/writer/config"
	controllerv1 "github.com/metrico/qryn/v4/writer/controller"
	"github.com/metrico/qryn/v4/writer/plugin"
)

// params for  Services
type ServicesObject struct {
}

func Init(cfg *clconfig.ClokiConfig, router *mux.Router) {
	/* first check admin flags */
	config.Cloki = cfg

	qrynPlugin := &plugin.QrynWriterPlugin{}

	qrynPlugin.Initialize(*config.Cloki.Setting)
	qrynPlugin.CreateStaticServiceRegistry(*config.Cloki.Setting)

	go qrynPlugin.StartPushStat()
	controllerv1.Registry = plugin.ServiceRegistry
	controllerv1.FPCache = plugin.GoCache

	proMiddlewareConfig := controllerv1.NewMiddlewareConfig(controllerv1.WithExtraMiddlewareDefault...)
	tempoMiddlewareConfig := controllerv1.NewMiddlewareConfig(controllerv1.WithExtraMiddlewareTempo...)

	qrynPlugin.RegisterRoutes(*config.Cloki.Setting, proMiddlewareConfig, tempoMiddlewareConfig, router)
}
