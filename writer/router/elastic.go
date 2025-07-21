package apirouterv1

import (
	"github.com/gorilla/mux"
	controllerv1 "github.com/metrico/qryn/writer/controller"
)

func RouteElasticDataApis(router *mux.Router, cfg controllerv1.MiddlewareConfig) {
	router.HandleFunc("/{target}/_doc", controllerv1.TargetDocV2(cfg)).Methods("POST")
	router.HandleFunc("/{target}/_create/{id}", controllerv1.TargetDocV2(cfg)).Methods("POST")
	router.HandleFunc("/{target}/_doc/{id}", controllerv1.TargetDocV2(cfg)).Methods("PUT")
	router.HandleFunc("/{target}/_create/{id}", controllerv1.TargetDocV2(cfg)).Methods("PUT")
	router.HandleFunc("/_bulk", controllerv1.TargetBulkV2(cfg)).Methods("POST")
	router.HandleFunc("/{target}/_bulk", controllerv1.TargetBulkV2(cfg)).Methods("POST")
}
