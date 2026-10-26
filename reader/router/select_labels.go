package router

import (
	"net/http"

	controllerv1 "github.com/metrico/qryn/v5/reader/controller"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/service"
)

func RouteSelectLabels(app *http.ServeMux, dataSession model.IDBRegistry) {
	qrService := service.NewQueryLabelsService(&model.ServiceData{
		Session: dataSession,
	})
	qrCtrl := &controllerv1.QueryLabelsController{
		QueryLabelsService: qrService,
	}
	app.HandleFunc("GET /loki/api/v1/label", qrCtrl.Labels)
	app.HandleFunc("POST /loki/api/v1/label", qrCtrl.Labels)
	app.HandleFunc("GET /loki/api/v1/labels", qrCtrl.Labels)
	app.HandleFunc("POST /loki/api/v1/labels", qrCtrl.Labels)
	app.HandleFunc("GET /loki/api/v1/label/{name}/values", qrCtrl.Values)
	app.HandleFunc("POST /loki/api/v1/label/{name}/values", qrCtrl.Values)
	app.HandleFunc("GET /loki/api/v1/series", qrCtrl.Series)
	app.HandleFunc("POST /loki/api/v1/series", qrCtrl.Series)
}
