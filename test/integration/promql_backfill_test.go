//go:build integration

package integration

import (
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/writer/utils/proto/prompb"
)

// Two hours of a series backfilled through remote write in seconds, as the compliance
// harness replays 1h50m of history in minutes, answer PromQL at the end of the backfill.
func TestPromQLAnswersAtTheEndOfABackfill(t *testing.T) {
	waitReady(t)
	name := fmt.Sprintf("it_backfill_%d", time.Now().UnixNano())
	t0 := time.Now().Add(-2 * time.Hour).Truncate(time.Minute).UnixMilli()
	const total, perRequest = 480, 60
	for from := 0; from < total; from += perRequest {
		var samples []*prompb.Sample
		for n := from; n < from+perRequest; n++ {
			samples = append(samples, &prompb.Sample{Timestamp: t0 + int64(n)*15000, Value: float64(n)})
		}
		remoteWrite(t, &prompb.TimeSeries{Labels: []*prompb.Label{{Name: "__name__", Value: name},
			{Name: "job", Value: "backfill"}}, Samples: samples})
	}
	eventually(t, fmt.Sprintf("SELECT count() FROM metric_samples FINAL WHERE fingerprint IN "+
		"(SELECT fingerprint FROM metric_series WHERE name = '%s')", name), fmt.Sprint(total))

	end := t0 + (total-1)*15000
	rng := promGet(t, "/api/v1/query_range", url.Values{"query": {name},
		"start": {fmt.Sprint((end - 600000) / 1000)}, "end": {fmt.Sprint(end / 1000)}, "step": {"60"}})
	if len(rng.Data.Result) != 1 {
		t.Fatalf("query_range %s over the last 10m of the backfill: %d series, want 1", name, len(rng.Data.Result))
	}
	values := rng.Data.Result[0].Values
	if len(values) != 11 {
		t.Fatalf("query_range %s: %d points, want 11", name, len(values))
	}
	if got, want := values[len(values)-1][1], fmt.Sprint(total-1); got != want {
		t.Errorf("query_range %s at the end of the backfill = %v, want %s", name, got, want)
	}
}
