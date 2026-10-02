package metricread

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
)

// probe is the probe dataset's window: (2026-01-01 00:00, 00:10].
var probe = Window{FromMs: 1767225600000, ToMs: 1767226200000}

func TestSeriesSQL(t *testing.T) {
	got := SeriesSQL(probe, []*labels.Matcher{matcher(labels.MatchEqual, "__name__", "x")})
	want := "SELECT fingerprint, any(labels) AS label_set FROM metric_series " +
		"WHERE (name = 'x') " +
		"AND last_seen >= fromUnixTimestamp64Milli(1767223800000) " +
		"AND first_seen <= fromUnixTimestamp64Milli(1767226200000) " +
		"GROUP BY fingerprint"
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestRawSamplesSQL(t *testing.T) {
	got := RawSamplesSQL(probe, []*labels.Matcher{matcher(labels.MatchEqual, "__name__", "x")})
	want := "SELECT fingerprint, timestamp, value FROM metric_samples " +
		"WHERE " + localSeries(1767223800000, 1767226200000) + " " +
		"AND timestamp > fromUnixTimestamp64Milli(1767225600000) " +
		"AND timestamp <= fromUnixTimestamp64Milli(1767226200000) " +
		"ORDER BY fingerprint, timestamp " +
		"LIMIT 1 BY fingerprint, timestamp"
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}
