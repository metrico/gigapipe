package apirouterv1

import (
	"github.com/gorilla/mux"
	controllerv1 "github.com/metrico/qryn/writer/controller"
)

func RoutePromDataApis(router *mux.Router, cfg controllerv1.MiddlewareConfig) {
	router.HandleFunc("/v1/prom/remote/write", controllerv1.WriteStreamV2(cfg)).Methods("POST")
	router.HandleFunc("/api/v1/prom/remote/write", controllerv1.WriteStreamV2(cfg)).Methods("POST")
	router.HandleFunc("/prom/remote/write", controllerv1.WriteStreamV2(cfg)).Methods("POST")
	router.HandleFunc("/api/prom/remote/write", controllerv1.WriteStreamV2(cfg)).Methods("POST")
	router.HandleFunc("/api/prom/push", controllerv1.WriteStreamV2(cfg)).Methods("POST")
	router.HandleFunc("/prom/remote/write", controllerv1.StatusOK(cfg)).Methods("GET")
}
