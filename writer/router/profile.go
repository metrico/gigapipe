package apirouterv1

import (
	"net/http"

	controllerv1 "github.com/metrico/qryn/v5/writer/controller"
)

func RouteProfileDataApis(router *http.ServeMux, cfg controllerv1.MiddlewareConfig) {

	router.HandleFunc("POST /ingest", controllerv1.PushProfileV2(cfg))

	router.HandleFunc("POST /v1development/profiles", controllerv1.OTLPProfilesV2(cfg))

}
