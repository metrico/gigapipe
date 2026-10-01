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
	"github.com/metrico/qryn/v5/writer/utils/unmarshal"
)

func TestPushMetricSeriesReachesTheMetricServices(t *testing.T) {
	installConfig(t)
	config.Cloki.Setting.FingerPrintType = clcwriter.FINGERPRINT_CityHash
	installFPCache(t, "n")
	oldCaches := MetricCaches
	MetricCaches = metriccache.New()
	t.Cleanup(func() {
		MetricCaches.Stop()
		MetricCaches = oldCaches
	})
	staging, series, meta := &recorderSvc{}, &recorderSvc{}, &recorderSvc{}
	one := func(s service.IInsertServiceV2) map[string]service.IInsertServiceV2 {
		return map[string]service.IInsertServiceV2{"n": s}
	}
	oldRegistry := Registry
	Registry = registry.NewStaticServiceRegistry(registry.StaticServiceRegistryOpts{
		MetricStagingSvcs: one(staging),
		MetricSeriesSvcs:  one(series),
		MetricMetaSvcs:    one(meta),
		MetricExmplSvcs:   one(&recorderSvc{}),
	})
	t.Cleanup(func() { Registry = oldRegistry })

	const staleBits = 0x7ff0000000000002
	err := PushMetricSeries(context.Background(), []unmarshal.MetricSeries{{
		Labels:       [][]string{{"__name__", "job:up:sum"}, {"job", "api"}},
		TimestampsMs: []int64{1000, 2000},
		Values:       []float64{3, math.Float64frombits(staleBits)},
	}})
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
	sd := series.reqs()[0].(*model.MetricSeriesData)
	if sd.MName[0] != "job:up:sum" || sd.MLabels[0]["job"] != "api" || sd.MFingerprint[0] != d.MFingerprint[0] {
		t.Fatalf("series row: name %q, labels %v", sd.MName[0], sd.MLabels[0])
	}
	if n := len(meta.reqs()); n != 0 {
		t.Fatalf("metadata requests: got %d, want 0", n)
	}
}
