package apirouterv1

import (
	"net/http"

	controllerv1 "github.com/metrico/qryn/v5/writer/controller"
)

func RoutePromDataApis(router *http.ServeMux, cfg controllerv1.MiddlewareConfig) {
	router.HandleFunc("POST /v1/prom/remote/write", controllerv1.WriteStreamV2(cfg))
	router.HandleFunc("POST /api/v1/prom/remote/write", controllerv1.WriteStreamV2(cfg))
	router.HandleFunc("POST /prom/remote/write", controllerv1.WriteStreamV2(cfg))
	router.HandleFunc("POST /api/prom/remote/write", controllerv1.WriteStreamV2(cfg))
	router.HandleFunc("POST /api/prom/push", controllerv1.WriteStreamV2(cfg))
	router.HandleFunc("GET /prom/remote/write", controllerv1.WriteStreamProbeV2)
}
