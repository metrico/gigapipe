package apirouterv1

import (
	"net/http"

	controllerv1 "github.com/metrico/qryn/v5/writer/controller"
)

func RouteMiscApis(router *http.ServeMux, cfg controllerv1.MiddlewareConfig) {

	//// todo need to remove below commented code
	//handler := promhttp.Handler()
	//router.RouterHandleFunc(http.MethodGet, "/ready", controllerv1.Ready)
	//router.RouterHandleFunc(http.MethodGet, "/metrics", func(r *http.Request, w http.ResponseWriter) error {
	//	handler.ServeHTTP(w, r)
	//	return nil
	//})
	//router.RouterHandleFunc(http.MethodGet, "/config", controllerv1.Config)
}
