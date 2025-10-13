package apirouterv1

import (
	"github.com/gorilla/mux"
	controllerv1 "github.com/metrico/qryn/v4/writer/controller"
)

func RouteInsertTempoApis(router *mux.Router, cfg controllerv1.MiddlewareConfig) {
	router.HandleFunc("/tempo/spans", controllerv1.PushV2(cfg)).Methods("POST")
	router.HandleFunc("/tempo/api/push", controllerv1.ClickhousePushV2(cfg)).Methods("POST")
	router.HandleFunc("/api/v2/spans", controllerv1.PushV2(cfg)).Methods("POST")
	router.HandleFunc("/v1/traces", controllerv1.OTLPPushV2(cfg)).Methods("POST")
}
