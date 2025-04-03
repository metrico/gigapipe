package apirouterv1

import (
	"github.com/gorilla/mux"
	controllerv1 "github.com/metrico/qryn/reader/controller"
)

func RouteMiscApis(app *mux.Router) {
	m := &controllerv1.MiscController{
		Version: "",
	}
	//app.HandleFunc("/ready", m.Ready).Methods("GET")
	//app.HandleFunc("/config", m.Config).Methods("GET")
	app.HandleFunc("/api/v1/metadata", m.Metadata).Methods("GET")
	app.HandleFunc("/api/v1/status/buildinfo", m.Buildinfo).Methods("GET")
	//app.Handle("/metrics", promhttp.Handler()).Methods("GET")
}
