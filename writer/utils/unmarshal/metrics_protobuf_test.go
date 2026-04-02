package unmarshal

import (
	"fmt"
	"testing"

	"github.com/metrico/qryn/v4/writer/utils/proto/prompb"
)

// TestPromMetricsFlushLimitTypesLength verifies that when a time series has more
// samples than flushLimit (1000), the types slice passed to onEntries always has
// the same length as the timestampsNS slice (not the total sample count).
//
// This is a regression test for: https://github.com/metrico/gigapipe/issues/783
func TestPromMetricsFlushLimitTypesLength(t *testing.T) {
	const totalSamples = 1001 // one more than flushLimit to trigger the mid-flush

	samples := make([]*prompb.Sample, totalSamples)
	for i := range samples {
		samples[i] = &prompb.Sample{Timestamp: int64(i), Value: float64(i)}
	}

	req := &prompb.WriteRequest{
		Timeseries: []*prompb.TimeSeries{
			{
				Labels:  []*prompb.Label{{Name: "__name__", Value: "test_metric"}},
				Samples: samples,
			},
		},
	}

	dec := &promMetricsProtoDec{
		ctx: &ParserCtx{bodyObject: req},
	}

	var mismatches []string
	dec.SetOnEntries(func(labels [][]string, timestampsNS []int64, message []string, value []float64, types []uint8) error {
		if len(types) != len(timestampsNS) {
			mismatches = append(mismatches, fmt.Sprintf(
				"types length %d != timestampsNS length %d", len(types), len(timestampsNS),
			))
		}
		return nil
	})

	if err := dec.Decode(); err != nil {
		t.Fatalf("Decode returned error: %v", err)
	}

	if len(mismatches) > 0 {
		for _, m := range mismatches {
			t.Errorf("column length mismatch: %s", m)
		}
	}
}
