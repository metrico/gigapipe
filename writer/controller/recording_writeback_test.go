package controller

import (
	"context"
	"testing"

	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/utils/proto/prompb"
)

// With the split on, a recording-rule write-back carries only metric rows, so
// they must reach the metrics service and never the samples service -- the
// same routing IngestParsed gives every other transport.
func TestPushPromWriteRequestRoutesToMetricsService(t *testing.T) {
	installFPCache(t, "n")
	installConfig(t)
	t.Setenv("SAMPLES_SPLIT_BY_SIGNAL", "true")
	reloadSamplesConfig(t)

	spl := &recorderSvc{}
	ts := &recorderSvc{}
	mtr := &recorderSvc{}
	installRegistry(t, spl, ts, mtr)

	wr := &prompb.WriteRequest{
		Timeseries: []*prompb.TimeSeries{
			{
				Labels:  []*prompb.Label{{Name: "__name__", Value: "test_rule"}},
				Samples: []*prompb.Sample{{Value: 1, Timestamp: 1}},
			},
		},
	}

	if err := PushPromWriteRequest(context.Background(), wr); err != nil {
		t.Fatalf("PushPromWriteRequest: %v", err)
	}

	if len(spl.reqs()) != 0 {
		t.Errorf("samples service got %d requests, want 0", len(spl.reqs()))
	}
	mtrReqs := mtr.reqs()
	if len(mtrReqs) != 1 {
		t.Fatalf("metrics service got %d requests, want 1", len(mtrReqs))
	}
	if got := mtrReqs[0].(*model.TimeSamplesData).MFingerprint; len(got) != 1 {
		t.Errorf("metric fingerprints = %v, want 1 row", got)
	}
	if len(ts.reqs()) != 1 {
		t.Errorf("time series service got %d requests, want 1", len(ts.reqs()))
	}
}
