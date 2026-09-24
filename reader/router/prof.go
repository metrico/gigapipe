package router

import (
	"net/http"

	controllerv1 "github.com/metrico/qryn/v5/reader/controller"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/prof"
	"github.com/metrico/qryn/v5/reader/service"
)

func RouteProf(app *http.ServeMux, dataSession model.IDBRegistry) {
	ctrl := controllerv1.ProfController{ProfService: &service.ProfService{DataSession: dataSession}}
	app.HandleFunc("POST "+prof.QuerierService_ProfileTypes_FullMethodName, ctrl.ProfileTypes)
	app.HandleFunc("POST "+prof.QuerierService_LabelNames_FullMethodName, ctrl.LabelNames)
	app.HandleFunc("POST "+prof.QuerierService_LabelValues_FullMethodName, ctrl.LabelValues)
	app.HandleFunc("POST "+prof.QuerierService_SelectMergeStacktraces_FullMethodName, ctrl.SelectMergeStackTraces)
	app.HandleFunc("POST "+prof.QuerierService_SelectSeries_FullMethodName, ctrl.SelectSeries)
	app.HandleFunc("POST "+prof.QuerierService_SelectMergeProfile_FullMethodName, ctrl.MergeProfiles)
	app.HandleFunc("POST "+prof.QuerierService_Series_FullMethodName, ctrl.Series)
	app.HandleFunc("POST "+prof.QuerierService_GetProfileStats_FullMethodName, ctrl.ProfileStats)
	app.HandleFunc("POST "+prof.SettingsService_Get_FullMethodName, ctrl.Settings)
	app.HandleFunc("POST "+prof.QuerierService_AnalyzeQuery_FullMethodName, ctrl.AnalyzeQuery)
	app.HandleFunc("GET /pyroscope/render", ctrl.Render)
	app.HandleFunc("GET /pyroscope/render-diff", ctrl.RenderDiff)
}
