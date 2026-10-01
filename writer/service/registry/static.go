package registry

import (
	"math/rand/v2"

	"github.com/metrico/qryn/v5/writer/service"
)

type staticServiceRegistry struct {
	TimeSeriesSvcs    []service.IInsertServiceV2
	SamplesSvcs       []service.IInsertServiceV2
	MetricStagingSvcs []service.IInsertServiceV2
	MetricSeriesSvcs  []service.IInsertServiceV2
	MetricMetaSvcs    []service.IInsertServiceV2
	MetricExmplSvcs   []service.IInsertServiceV2
	TempoSamplesSvcs  []service.IInsertServiceV2
	TempoTagsSvcs     []service.IInsertServiceV2
	ProfileInsertSvcs []service.IInsertServiceV2
	PatternInsertSvcs []service.IInsertServiceV2
}

type StaticServiceRegistryOpts struct {
	TimeSeriesSvcs    map[string]service.IInsertServiceV2
	SamplesSvcs       map[string]service.IInsertServiceV2
	MetricStagingSvcs map[string]service.IInsertServiceV2
	MetricSeriesSvcs  map[string]service.IInsertServiceV2
	MetricMetaSvcs    map[string]service.IInsertServiceV2
	MetricExmplSvcs   map[string]service.IInsertServiceV2
	TempoSamplesSvcs  map[string]service.IInsertServiceV2
	TempoTagsSvcs     map[string]service.IInsertServiceV2
	ProfileInsertSvcs map[string]service.IInsertServiceV2
	PatternInsertSvcs map[string]service.IInsertServiceV2
}

func mapToSlice(m map[string]service.IInsertServiceV2) []service.IInsertServiceV2 {
	var ss []service.IInsertServiceV2
	for _, s := range m {
		ss = append(ss, s)
	}
	return ss
}

func NewStaticServiceRegistry(opts StaticServiceRegistryOpts) ServiceRegistry {
	res := staticServiceRegistry{
		TimeSeriesSvcs:    mapToSlice(opts.TimeSeriesSvcs),
		SamplesSvcs:       mapToSlice(opts.SamplesSvcs),
		MetricStagingSvcs: mapToSlice(opts.MetricStagingSvcs),
		MetricSeriesSvcs:  mapToSlice(opts.MetricSeriesSvcs),
		MetricMetaSvcs:    mapToSlice(opts.MetricMetaSvcs),
		MetricExmplSvcs:   mapToSlice(opts.MetricExmplSvcs),
		TempoSamplesSvcs:  mapToSlice(opts.TempoSamplesSvcs),
		TempoTagsSvcs:     mapToSlice(opts.TempoTagsSvcs),
		ProfileInsertSvcs: mapToSlice(opts.ProfileInsertSvcs),
		PatternInsertSvcs: mapToSlice(opts.PatternInsertSvcs),
	}
	return &res
}

func staticServiceRegistryGetService[T interface{ GetNodeName() string }](r *staticServiceRegistry, id string,
	svcs []T,
) (T, error) {
	if id != "" {
		for _, svc := range svcs {
			if svc.GetNodeName() == id {
				return svc, nil
			}
		}
	}
	idx := rand.IntN(len(svcs))
	return svcs[idx], nil
}

func (r *staticServiceRegistry) getService(id string,
	svcs []service.IInsertServiceV2,
) (service.IInsertServiceV2, error) {
	return staticServiceRegistryGetService(r, id, svcs)
}

func (r *staticServiceRegistry) GetTimeSeriesService(id string) (service.IInsertServiceV2, error) {
	return r.getService(id, r.TimeSeriesSvcs)
}

func (r *staticServiceRegistry) GetSamplesService(id string) (service.IInsertServiceV2, error) {
	return r.getService(id, r.SamplesSvcs)
}

func (r *staticServiceRegistry) GetMetricStagingService(id string) (service.IInsertServiceV2, error) {
	return r.getService(id, r.MetricStagingSvcs)
}

func (r *staticServiceRegistry) GetMetricSeriesService(id string) (service.IInsertServiceV2, error) {
	return r.getService(id, r.MetricSeriesSvcs)
}

func (r *staticServiceRegistry) GetMetricMetadataService(id string) (service.IInsertServiceV2, error) {
	return r.getService(id, r.MetricMetaSvcs)
}

func (r *staticServiceRegistry) GetMetricExemplarsService(id string) (service.IInsertServiceV2, error) {
	return r.getService(id, r.MetricExmplSvcs)
}

func (r *staticServiceRegistry) GetSpansService(id string) (service.IInsertServiceV2, error) {
	return r.getService(id, r.TempoSamplesSvcs)
}

func (r *staticServiceRegistry) GetSpansSeriesService(id string) (service.IInsertServiceV2, error) {
	return r.getService(id, r.TempoTagsSvcs)
}

func (r *staticServiceRegistry) GetProfileInsertService(id string) (service.IInsertServiceV2, error) {
	return r.getService(id, r.ProfileInsertSvcs)
}

func (r *staticServiceRegistry) GetPatternInsertService(id string) (service.IInsertServiceV2, error) {
	return r.getService(id, r.PatternInsertSvcs)
}

func (r *staticServiceRegistry) Run() {}

func (r *staticServiceRegistry) Stop() {}
