package apirouterv1

import (
	"github.com/gorilla/mux"
	controllerv1 "github.com/metrico/qryn/v4/writer/controller"
)

func RouteProfileDataApis(router *mux.Router, cfg controllerv1.MiddlewareConfig) {

	router.HandleFunc("/ingest", controllerv1.PushProfileV2(cfg)).Methods("POST")

}
