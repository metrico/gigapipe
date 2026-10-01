package unmarshal

import (
	"context"
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/writer/utils/metriccache"
)

func TestDatadogMetricsReachTheMetricEntryPoint(t *testing.T) {
	withCityHashFingerprints(t)
	body := `{"series":[{"metric":"system.load.1","type":0,
		"points":[{"timestamp":1700000000,"value":0.5},{"timestamp":1700000010,"value":0.75}],
		"resources":[{"name":"host1","type":"host"}]}]}`
	ctx := metriccache.NewContext(context.Background(), newNode(t))
	rows := collectMetricRows(t, UnmarshallDatadogMetricsV2JSONV2(ctx, strings.NewReader(body), nil))

	if len(rows.series) != 1 {
		t.Fatalf("series rows: got %+v, want 1", rows.series)
	}
	s := rows.series[0]
	wantLabels := map[string]string{"__name__": "system.load.1", "resource1_name": "host1",
		"resource1_type": "host", "service_name": "unknown"}
	if s.name != "system.load.1" || len(s.labels) != len(wantLabels) {
		t.Fatalf("series row: got %+v, want labels %v", s, wantLabels)
	}
	for k, v := range wantLabels {
		if s.labels[k] != v {
			t.Errorf("label %s: got %q, want %q", k, s.labels[k], v)
		}
	}
	if len(rows.staging) != 2 {
		t.Fatalf("staging rows: got %+v, want 2", rows.staging)
	}
	for i, want := range []struct {
		tsMs  int64
		value float64
	}{{1700000000_000, 0.5}, {1700000010_000, 0.75}} {
		r := rows.staging[i]
		if r.fp != s.fp || r.tsMs != want.tsMs || r.value != want.value {
			t.Errorf("staging row %d: got %+v, want ts %d value %v", i, r, want.tsMs, want.value)
		}
	}
}
