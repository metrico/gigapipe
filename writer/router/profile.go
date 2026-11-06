package apirouterv1

import (
	"net/http"

	controllerv1 "github.com/metrico/qryn/v5/writer/controller"
)

func RouteProfileDataApis(router *http.ServeMux, cfg controllerv1.MiddlewareConfig) {

	router.HandleFunc("POST /ingest", controllerv1.PushProfileV2(cfg))

	router.HandleFunc("POST /v1development/profiles", controllerv1.OTLPProfilesV2(cfg))

	// The Pyroscope v2 push API. Every SDK from 1.0 onwards sends here over
	// Connect; /ingest above is the legacy upload that older releases used.
	router.HandleFunc("/push.v1.PusherService/Push", controllerv1.PushProfileV1(cfg)).Methods("POST")

}
