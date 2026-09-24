package apirouterv1

import (
	"net/http"

	controllerv1 "github.com/metrico/qryn/v5/writer/controller"
)

func RouteInsertDataApis(router *http.ServeMux, cfg controllerv1.MiddlewareConfig) {
	router.HandleFunc("POST /loki/api/v1/push", controllerv1.PushStreamV2(cfg))
	router.HandleFunc("POST /influx/api/v2/write", controllerv1.PushInfluxV2(cfg))
	router.HandleFunc("POST /cf/v1/insert", controllerv1.PushCfDatadogV2(cfg))
	router.HandleFunc("POST /api/v2/series", controllerv1.PushDatadogMetricsV2(cfg))
	router.HandleFunc("POST /api/v2/logs", controllerv1.PushDatadogV2(cfg))
	router.HandleFunc("POST /v1/logs", controllerv1.OTLPLogsV2(cfg))
	router.HandleFunc("POST /v1/metrics", controllerv1.OTLPMetricsV2(cfg))

	router.HandleFunc("GET /influx/api/v2/write/health", controllerv1.HealthInflux)
	router.HandleFunc("GET /influx/health", controllerv1.HealthInflux)

}
