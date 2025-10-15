package insert

import (
	"github.com/metrico/qryn/v4/writer/model"
	"github.com/metrico/qryn/v4/writer/service"
)

type DevInsertServiceFactory struct{}

func (f *DevInsertServiceFactory) NewTimeSeriesInsertService(opts model.InsertServiceOpts) service.IInsertServiceV2 {
	return NewTimeSeriesInsertService(opts)
}

func (f *DevInsertServiceFactory) NewSamplesInsertService(opts model.InsertServiceOpts) service.IInsertServiceV2 {
	return NewSamplesInsertService(opts)
}

func (f *DevInsertServiceFactory) NewMetricsInsertService(opts model.InsertServiceOpts) service.IInsertServiceV2 {
	return NewMetricsInsertService(opts)
}

func (f *DevInsertServiceFactory) NewTempoSamplesInsertService(opts model.InsertServiceOpts) service.IInsertServiceV2 {
	return NewTempoSamplesInsertService(opts)
}

func (f *DevInsertServiceFactory) NewTempoTagInsertService(opts model.InsertServiceOpts) service.IInsertServiceV2 {
	return NewTempoTagsInsertService(opts)
}

func (f *DevInsertServiceFactory) NewProfileSamplesInsertService(opts model.InsertServiceOpts) service.IInsertServiceV2 {
	return NewProfileSamplesInsertService(opts)
}
