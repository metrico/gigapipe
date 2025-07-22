package plugins

import (
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/writer/chwrapper"
	"github.com/metrico/qryn/writer/model"
	"github.com/metrico/qryn/writer/service"
)

type NewTempoTracesService = func(opts model.InsertServiceOpts) service.IInsertServiceV2
type NewSamplesInsertService = func(opts model.InsertServiceOpts) service.IInsertServiceV2
type NewProfileInsertService = func(opts model.InsertServiceOpts) service.IInsertServiceV2
type NewMetricInsertService = func(opts model.InsertServiceOpts) service.IInsertServiceV2
type NewTimeSeriesInsertService = func(opts model.InsertServiceOpts) service.IInsertServiceV2
type HealthCheck = func(conn chwrapper.IChClient, isDistributed bool)
type DatabaseSession = func(config config.ClokiBaseSettingServer) ([]model.DataDatabasesMap, []chwrapper.IChClient, []chwrapper.IChClientFactory)

const (
	tracesInsertServicePlugin  = "traces_insert"
	samplesInsertServicePlugin = "samples_insert"
	profileInsertServicePlugin = "profile_insert"
	metricInsertServicePlugin  = "metric_insert"
	timeInsertServicePlugin    = "time_insert"
	HealthCheckPlugin          = "health_check"
	databaseSessionPlugin      = "database_session"
)

var RegisterTracesInsertServicePlugin = registerPlugin[NewTempoTracesService](tracesInsertServicePlugin)
var GetTracesInsertServicePlugin = getPlugin[NewTempoTracesService](tracesInsertServicePlugin)

var RegisterSamplesInsertServicePlugin = registerPlugin[NewSamplesInsertService](samplesInsertServicePlugin)
var GetSamplesInsertServicePlugin = getPlugin[NewSamplesInsertService](samplesInsertServicePlugin)

var RegisterMetricInsertServicePlugin = registerPlugin[NewMetricInsertService](metricInsertServicePlugin)
var GetMetricInsertServicePlugin = getPlugin[NewMetricInsertService](metricInsertServicePlugin)

var RegisterTimeSeriesInsertServicePlugin = registerPlugin[NewTimeSeriesInsertService](timeInsertServicePlugin)
var GetTimeSeriesInsertServicePlugin = getPlugin[NewTimeSeriesInsertService](timeInsertServicePlugin)

var GetHealthCheckPlugin = getPlugin[HealthCheck](HealthCheckPlugin)
var RegisterHealthCheckPlugin = registerPlugin[HealthCheck](HealthCheckPlugin)

var RegisterDatabaseSessionPlugin = registerPlugin[DatabaseSession](databaseSessionPlugin)
var GetDatabaseSessionPlugin = getPlugin[DatabaseSession](databaseSessionPlugin)

var RegisterProfileInsertServicePlugin = registerPlugin[NewProfileInsertService](profileInsertServicePlugin)
var GetProfileInsertServicePlugin = getPlugin[NewProfileInsertService](profileInsertServicePlugin)
