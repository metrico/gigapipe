package router

import (
	"net/http"

	controllerv1 "github.com/metrico/qryn/v5/reader/controller"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/service"
)

func RouteSelectPrometheusLabels(app *http.ServeMux, dataSession model.IDBRegistry) {
	sd := &model.ServiceData{
		Session: dataSession,
	}
	qrService := service.NewQueryLabelsService(sd)
	metadataService := service.NewMetadataService(sd)
	qrCtrl := &controllerv1.PromQueryLabelsController{
		QueryLabelsService: qrService,
		MetadataService:    metadataService,
	}
	app.HandleFunc("GET /api/v1/labels", qrCtrl.PromLabels)
	app.HandleFunc("POST /api/v1/labels", qrCtrl.PromLabels)
	app.HandleFunc("GET /api/v1/label/{name}/values", qrCtrl.LabelValues)
	app.HandleFunc("GET /api/v1/metadata", qrCtrl.Metadata)
	app.HandleFunc("GET /api/v1/query_exemplars", qrCtrl.Metadata)
	// /api/v1/rules is owned by the ruler module (recording rules), which
	// registers it when enabled.
	app.HandleFunc("GET /api/v1/series", qrCtrl.Series)
	app.HandleFunc("POST /api/v1/series", qrCtrl.Series)
}
