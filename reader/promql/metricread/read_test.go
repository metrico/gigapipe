package metricread

import (
	"fmt"
	"strings"
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
	want := "SELECT fingerprint, timestamp, value FROM metric_samples FINAL " +
		"WHERE " + localSeries(1767223800000, 1767226200000) + " " +
		"AND timestamp > fromUnixTimestamp64Milli(1767225600000) " +
		"AND timestamp <= fromUnixTimestamp64Milli(1767226200000) " +
		"ORDER BY fingerprint, timestamp " +
		"SETTINGS do_not_merge_across_partitions_select_final = 1"
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

// pluginSeries is a series source that names its window.
func pluginSeries(w Window, matchers []*labels.Matcher) string {
	return fmt.Sprintf("SELECT fingerprint, label_set FROM plugin_series(%d, %d, %d)", len(matchers), w.FromMs, w.ToMs)
}

func TestSeriesSourceNamesTheSeriesOfEveryRead(t *testing.T) {
	selector := []*labels.Matcher{matcher(labels.MatchEqual, "__name__", "x")}
	w := probe
	w.Series = pluginSeries
	p := Pushdown{Grid: Grid{StartMs: 1767225900000, EndMs: 1767226200000, StepMs: 60000}, Func: "rate",
		RangeMs: 300000, Matchers: selector, Aggregation: &Aggregation{Op: "sum", Grouping: []string{"job"}},
		Series: pluginSeries}
	tierP := p
	tierP.Tier = Tier5m
	for name, tc := range map[string]struct{ sql, want string }{
		"series":          {SeriesSQL(w, selector), "SELECT fingerprint, label_set FROM plugin_series(1, 1767225600000, 1767226200000)"},
		"pushdown labels": {PushdownLabelsSQL(p), "FROM (SELECT fingerprint, label_set FROM plugin_series(1, 1767225600000, 1767226200000))"},
		"pushdown groups": {PushdownSQL(p), "FROM (SELECT fingerprint, label_set FROM plugin_series(1, 1767225600000, 1767226200000))"},
		"tier labels":     {PushdownLabelsSQL(tierP), "FROM (SELECT fingerprint, label_set FROM plugin_series(1, 1767225600000, 1767226200000))"},
		"tier groups":     {PushdownSQL(tierP), "FROM (SELECT fingerprint, label_set FROM plugin_series(1, 1767225600000, 1767226200000))"},
	} {
		if !strings.Contains(tc.sql, tc.want) {
			t.Errorf("%s: %s\nlacks %s", name, tc.sql, tc.want)
		}
		if strings.Contains(tc.sql, "any(labels)") {
			t.Errorf("%s reads labels from the series index: %s", name, tc.sql)
		}
	}
}
