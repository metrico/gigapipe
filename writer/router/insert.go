package apirouterv1

import (
	"github.com/gorilla/mux"
	controllerv1 "github.com/metrico/qryn/v4/writer/controller"
)

func RouteInsertDataApis(router *mux.Router, cfg controllerv1.MiddlewareConfig) {
	router.HandleFunc("/loki/api/v1/push", controllerv1.PushStreamV2(cfg)).Methods("POST")
	router.HandleFunc("/influx/api/v2/write", controllerv1.PushInfluxV2(cfg)).Methods("POST")
	router.HandleFunc("/cf/v1/insert", controllerv1.PushCfDatadogV2(cfg)).Methods("POST")
	router.HandleFunc("/api/v2/series", controllerv1.PushDatadogMetricsV2(cfg)).Methods("POST")
	router.HandleFunc("/api/v2/logs", controllerv1.PushDatadogV2(cfg)).Methods("POST")
	router.HandleFunc("/v1/logs", controllerv1.OTLPLogsV2(cfg)).Methods("POST")

	router.HandleFunc("/influx/api/v2/write/health", controllerv1.HealthInflux).Methods("GET")
	router.HandleFunc("/influx/health", controllerv1.HealthInflux).Methods("GET")

}
