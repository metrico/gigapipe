package apirouterv1

import (
	"net/http"

	controllerv1 "github.com/metrico/qryn/v5/writer/controller"
)

func RouteInsertTempoApis(router *http.ServeMux, cfg controllerv1.MiddlewareConfig) {
	router.HandleFunc("POST /tempo/spans", controllerv1.PushV2(cfg))
	router.HandleFunc("POST /tempo/api/push", controllerv1.ClickhousePushV2(cfg))
	router.HandleFunc("POST /api/v2/spans", controllerv1.PushV2(cfg))
	router.HandleFunc("POST /v1/traces", controllerv1.OTLPPushV2(cfg))
}
