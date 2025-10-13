package apirouterv1

import (
	"github.com/gorilla/mux"
	controllerv1 "github.com/metrico/qryn/v4/writer/controller"
)

func RouteMiscApis(router *mux.Router, cfg controllerv1.MiddlewareConfig) {

	//// todo need to remove below commented code
	//handler := promhttp.Handler()
	//router.RouterHandleFunc(http.MethodGet, "/ready", controllerv1.Ready)
	//router.RouterHandleFunc(http.MethodGet, "/metrics", func(r *http.Request, w http.ResponseWriter) error {
	//	handler.ServeHTTP(w, r)
	//	return nil
	//})
	//router.RouterHandleFunc(http.MethodGet, "/config", controllerv1.Config)
}
