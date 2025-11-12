package registry

import "github.com/metrico/qryn/v4/writer/service"

type ServiceRegistry interface {
	GetTimeSeriesService(id string) (service.IInsertServiceV2, error)
	GetSamplesService(id string) (service.IInsertServiceV2, error)
	GetMetricsService(id string) (service.IInsertServiceV2, error)
	GetSpansService(id string) (service.IInsertServiceV2, error)
	GetSpansSeriesService(id string) (service.IInsertServiceV2, error)
	GetProfileInsertService(id string) (service.IInsertServiceV2, error)
	GetPatternInsertService(id string) (service.IInsertServiceV2, error)
	GetMetadataService(id string) (service.IInsertServiceV2, error)
	Run()
	Stop()
}
