package registry

import (
	"math/rand"
	"sync"
	"time"

	"github.com/metrico/qryn/v4/writer/service"
)

type staticServiceRegistry struct {
	TimeSeriesSvcs    []service.IInsertServiceV2
	SamplesSvcs       []service.IInsertServiceV2
	MetricSvcs        []service.IInsertServiceV2
	TempoSamplesSvcs  []service.IInsertServiceV2
	TempoTagsSvcs     []service.IInsertServiceV2
	ProfileInsertSvcs []service.IInsertServiceV2
	PatternInsertSvcs []service.IInsertServiceV2
	rand              *rand.Rand
	mtx               sync.Mutex
}

type StaticServiceRegistryOpts struct {
	TimeSeriesSvcs    map[string]service.IInsertServiceV2
	SamplesSvcs       map[string]service.IInsertServiceV2
	MetricSvcs        map[string]service.IInsertServiceV2
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
		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	res.TimeSeriesSvcs = mapToSlice(opts.TimeSeriesSvcs)
	res.SamplesSvcs = mapToSlice(opts.SamplesSvcs)
	res.MetricSvcs = mapToSlice(opts.MetricSvcs)
	res.TempoSamplesSvcs = mapToSlice(opts.TempoSamplesSvcs)
	res.TempoTagsSvcs = mapToSlice(opts.TempoTagsSvcs)
	res.ProfileInsertSvcs = mapToSlice(opts.ProfileInsertSvcs)
	res.PatternInsertSvcs = mapToSlice(opts.PatternInsertSvcs)
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
	r.mtx.Lock()
	defer r.mtx.Unlock()
	idx := r.rand.Intn(len(svcs))
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

func (r *staticServiceRegistry) GetMetricsService(id string) (service.IInsertServiceV2, error) {
	return r.getService(id, r.MetricSvcs)
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
