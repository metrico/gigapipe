package router

import (
	"github.com/gorilla/mux"
	controllerv1 "github.com/metrico/qryn/reader/controller"
	"github.com/metrico/qryn/reader/model"
	"github.com/metrico/qryn/reader/prof"
	"github.com/metrico/qryn/reader/service"
)

func RouteProf(app *mux.Router, dataSession model.IDBRegistry) {
	ctrl := controllerv1.ProfController{ProfService: &service.ProfService{DataSession: dataSession}}
	app.HandleFunc(prof.QuerierService_ProfileTypes_FullMethodName, ctrl.ProfileTypes).Methods("POST", "OPTIONS")
	app.HandleFunc(prof.QuerierService_LabelNames_FullMethodName, ctrl.LabelNames).Methods("POST", "OPTIONS")
	app.HandleFunc(prof.QuerierService_LabelValues_FullMethodName, ctrl.LabelValues).Methods("POST", "OPTIONS")
	app.HandleFunc(prof.QuerierService_SelectMergeStacktraces_FullMethodName, ctrl.SelectMergeStackTraces).
		Methods("POST", "OPTIONS")
	app.HandleFunc(prof.QuerierService_SelectSeries_FullMethodName, ctrl.SelectSeries).Methods("POST", "OPTIONS")
	app.HandleFunc(prof.QuerierService_SelectMergeProfile_FullMethodName, ctrl.MergeProfiles).Methods("POST", "OPTIONS")
	app.HandleFunc(prof.QuerierService_Series_FullMethodName, ctrl.Series).Methods("POST", "OPTIONS")
	app.HandleFunc(prof.QuerierService_GetProfileStats_FullMethodName, ctrl.ProfileStats).Methods("POST", "OPTIONS")
	app.HandleFunc(prof.SettingsService_Get_FullMethodName, ctrl.Settings).Methods("POST", "OPTIONS")
	app.HandleFunc(prof.QuerierService_AnalyzeQuery_FullMethodName, ctrl.AnalyzeQuery).Methods("POST", "OPTIONS")
	// app.HandleFunc("/pyroscope/render", ctrl.NotImplemented).Methods("GET")
	app.HandleFunc("/pyroscope/render-diff", ctrl.RenderDiff).Methods("GET", "OPTIONS")
}
