//go:build integration

package integration

import (
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/writer/utils/proto/prompb"
)

// A series backfilled with two hours of history in seconds, as the compliance harness
// replays 1h50m in minutes, answers PromQL at the end of the backfill.
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

// A live series whose history then arrives in ascending requests answers PromQL in the
// middle of that history while metric_series rows stay unmerged.
func TestPromQLAnswersInsideHistoryWrittenAfterALiveSample(t *testing.T) {
	waitReady(t)
	clickhouseQuery(t, "SYSTEM STOP MERGES metric_series")
	t.Cleanup(func() { clickhouseQuery(t, "SYSTEM START MERGES metric_series") })
	name := fmt.Sprintf("it_late_history_%d", time.Now().UnixNano())
	labels := []*prompb.Label{{Name: "__name__", Value: name}, {Name: "job", Value: "backfill"}}
	live := time.Now().Truncate(time.Minute).UnixMilli()
	remoteWrite(t, &prompb.TimeSeries{Labels: labels, Samples: []*prompb.Sample{{Timestamp: live, Value: -1}}})
	eventually(t, fmt.Sprintf("SELECT count() FROM metric_series WHERE name = '%s'", name), "1")

	t0 := live - 2*time.Hour.Milliseconds()
	const total, perRequest = 460, 20
	for from := 0; from < total; from += perRequest {
		var samples []*prompb.Sample
		for n := from; n < from+perRequest; n++ {
			samples = append(samples, &prompb.Sample{Timestamp: t0 + int64(n)*15000, Value: float64(n)})
		}
		remoteWrite(t, &prompb.TimeSeries{Labels: labels, Samples: samples})
	}
	eventually(t, fmt.Sprintf("SELECT count() FROM metric_samples FINAL WHERE fingerprint IN "+
		"(SELECT fingerprint FROM metric_series WHERE name = '%s')", name), fmt.Sprint(total+1))

	start, end := live-time.Hour.Milliseconds(), live-50*time.Minute.Milliseconds()
	rng := promGet(t, "/api/v1/query_range", url.Values{"query": {name},
		"start": {fmt.Sprint(start / 1000)}, "end": {fmt.Sprint(end / 1000)}, "step": {"60"}})
	if len(rng.Data.Result) != 1 {
		t.Fatalf("query_range %s an hour before the live sample: %d series, want 1", name, len(rng.Data.Result))
	}
	values := rng.Data.Result[0].Values
	if len(values) != 11 {
		t.Fatalf("query_range %s: %d points, want 11", name, len(values))
	}
	if got, want := values[len(values)-1][1], fmt.Sprint((end-t0)/15000); got != want {
		t.Errorf("query_range %s at %d = %v, want %s", name, end, got, want)
	}
}
