package plugins

import (
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/writer/chwrapper"
	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/service"
)

type NewTempoTracesService = func(opts model.InsertServiceOpts) service.IInsertServiceV2
type NewSamplesInsertService = func(opts model.InsertServiceOpts) service.IInsertServiceV2
type NewProfileInsertService = func(opts model.InsertServiceOpts) service.IInsertServiceV2
type NewMetricStagingInsertService = func(opts model.InsertServiceOpts) service.IInsertServiceV2
type NewMetricSeriesInsertService = func(opts model.InsertServiceOpts) service.IInsertServiceV2
type NewMetricMetadataInsertService = func(opts model.InsertServiceOpts) service.IInsertServiceV2
type NewMetricExemplarsInsertService = func(opts model.InsertServiceOpts) service.IInsertServiceV2
type NewTimeSeriesInsertService = func(opts model.InsertServiceOpts) service.IInsertServiceV2
type HealthCheck = func(conn chwrapper.IChClient, isDistributed bool)
type DatabaseSession = func(config config.ClokiBaseSettingServer) ([]model.DataDatabasesMap, []chwrapper.IChClient, []chwrapper.IChClientFactory)

const (
	tracesInsertServicePlugin          = "traces_insert"
	samplesInsertServicePlugin         = "samples_insert"
	profileInsertServicePlugin         = "profile_insert"
	metricStagingInsertServicePlugin   = "metric_staging_insert"
	metricSeriesInsertServicePlugin    = "metric_series_insert"
	metricMetadataInsertServicePlugin  = "metric_metadata_insert"
	metricExemplarsInsertServicePlugin = "metric_exemplars_insert"
	timeInsertServicePlugin            = "time_insert"
	HealthCheckPlugin                  = "health_check"
	databaseSessionPlugin              = "database_session"
)

var RegisterTracesInsertServicePlugin = registerPlugin[NewTempoTracesService](tracesInsertServicePlugin)
var GetTracesInsertServicePlugin = getPlugin[NewTempoTracesService](tracesInsertServicePlugin)

var RegisterSamplesInsertServicePlugin = registerPlugin[NewSamplesInsertService](samplesInsertServicePlugin)
var GetSamplesInsertServicePlugin = getPlugin[NewSamplesInsertService](samplesInsertServicePlugin)

var RegisterMetricStagingInsertServicePlugin = registerPlugin[NewMetricStagingInsertService](metricStagingInsertServicePlugin)
var GetMetricStagingInsertServicePlugin = getPlugin[NewMetricStagingInsertService](metricStagingInsertServicePlugin)

var RegisterMetricSeriesInsertServicePlugin = registerPlugin[NewMetricSeriesInsertService](metricSeriesInsertServicePlugin)
var GetMetricSeriesInsertServicePlugin = getPlugin[NewMetricSeriesInsertService](metricSeriesInsertServicePlugin)

var RegisterMetricMetadataInsertServicePlugin = registerPlugin[NewMetricMetadataInsertService](metricMetadataInsertServicePlugin)
var GetMetricMetadataInsertServicePlugin = getPlugin[NewMetricMetadataInsertService](metricMetadataInsertServicePlugin)

var RegisterMetricExemplarsInsertServicePlugin = registerPlugin[NewMetricExemplarsInsertService](metricExemplarsInsertServicePlugin)
var GetMetricExemplarsInsertServicePlugin = getPlugin[NewMetricExemplarsInsertService](metricExemplarsInsertServicePlugin)

var RegisterTimeSeriesInsertServicePlugin = registerPlugin[NewTimeSeriesInsertService](timeInsertServicePlugin)
var GetTimeSeriesInsertServicePlugin = getPlugin[NewTimeSeriesInsertService](timeInsertServicePlugin)

var GetHealthCheckPlugin = getPlugin[HealthCheck](HealthCheckPlugin)
var RegisterHealthCheckPlugin = registerPlugin[HealthCheck](HealthCheckPlugin)

var RegisterDatabaseSessionPlugin = registerPlugin[DatabaseSession](databaseSessionPlugin)
var GetDatabaseSessionPlugin = getPlugin[DatabaseSession](databaseSessionPlugin)

var RegisterProfileInsertServicePlugin = registerPlugin[NewProfileInsertService](profileInsertServicePlugin)
var GetProfileInsertServicePlugin = getPlugin[NewProfileInsertService](profileInsertServicePlugin)
