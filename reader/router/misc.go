package router

import (
	"net/http"

	controllerv1 "github.com/metrico/qryn/v5/reader/controller"
)

func RouteMiscApis(app *http.ServeMux) {
	m := &controllerv1.MiscController{
		Version: "",
	}
	app.HandleFunc("GET /api/v1/status/buildinfo", m.Buildinfo)
}
