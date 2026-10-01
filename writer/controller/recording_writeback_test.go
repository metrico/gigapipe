package controller

import (
	"context"
	"math"
	"testing"

	clcwriter "github.com/metrico/cloki-config/config/writer"
	"github.com/metrico/qryn/v5/writer/config"
	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/service"
	"github.com/metrico/qryn/v5/writer/service/registry"
	"github.com/metrico/qryn/v5/writer/utils/metriccache"
	"github.com/metrico/qryn/v5/writer/utils/proto/prompb"
)

func TestPushPromWriteRequestReachesTheStagingService(t *testing.T) {
	installConfig(t)
	config.Cloki.Setting.FingerPrintType = clcwriter.FINGERPRINT_CityHash
	installFPCache(t, "n")
	oldCaches := MetricCaches
	MetricCaches = metriccache.New()
	t.Cleanup(func() {
		MetricCaches.Stop()
		MetricCaches = oldCaches
	})
	staging, series := &recorderSvc{}, &recorderSvc{}
	one := func(s service.IInsertServiceV2) map[string]service.IInsertServiceV2 {
		return map[string]service.IInsertServiceV2{"n": s}
	}
	oldRegistry := Registry
	Registry = registry.NewStaticServiceRegistry(registry.StaticServiceRegistryOpts{
		MetricStagingSvcs: one(staging),
		MetricSeriesSvcs:  one(series),
		MetricMetaSvcs:    one(&recorderSvc{}),
		MetricExmplSvcs:   one(&recorderSvc{}),
	})
	t.Cleanup(func() { Registry = oldRegistry })

	const staleBits = 0x7ff0000000000002
	err := PushPromWriteRequest(context.Background(), &prompb.WriteRequest{Timeseries: []*prompb.TimeSeries{{
		Labels: []*prompb.Label{{Name: "__name__", Value: "job:up:sum"}},
		Samples: []*prompb.Sample{
			{Timestamp: 1000, Value: 3},
			{Timestamp: 2000, Value: math.Float64frombits(staleBits)},
		},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	reqs := staging.reqs()
	if len(reqs) != 1 {
		t.Fatalf("staging requests: got %d, want 1", len(reqs))
	}
	d := reqs[0].(*model.MetricSamplesData)
	if len(d.MValue) != 2 || d.MValue[0] != 3 || math.Float64bits(d.MValue[1]) != staleBits {
		t.Fatalf("staging values: got %v", d.MValue)
	}
	if d.MPrevTimestampMs[1] != 1000 || d.MPrevValue[1] != 3 || d.MAggregate[1] != 1 {
		t.Fatalf("stale marker row: prev (%d, %v), aggregate %d", d.MPrevTimestampMs[1], d.MPrevValue[1], d.MAggregate[1])
	}
	if len(series.reqs()) != 1 {
		t.Fatalf("series requests: got %d, want 1", len(series.reqs()))
	}
}
