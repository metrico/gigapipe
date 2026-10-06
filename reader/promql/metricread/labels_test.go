package metricread

import (
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
)

// probeStart and probeEnd bound the label probe: 2026-10-01 00:00 to 12:00.
var (
	probeStart int64 = 1790812800000
	probeEnd   int64 = 1790856000000
)

func TestLabelNamesSQLNarrowsUnderASelector(t *testing.T) {
	got := LabelNamesSQL(IndexQuery{
		Selectors: [][]*labels.Matcher{{matcher(labels.MatchEqual, "__name__", "up")}},
		StartMs:   &probeStart,
		EndMs:     &probeEnd,
		Limit:     100,
	})
	want := "SELECT DISTINCT arrayJoin(mapKeys(labels)) AS label FROM metric_series " +
		"WHERE last_seen >= fromUnixTimestamp64Milli(1790811000000) " +
		"AND first_seen <= fromUnixTimestamp64Milli(1790856000000) " +
		"AND (name = 'up') " +
		"ORDER BY label LIMIT 101"
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestLabelNamesSQLWithoutBoundsSelectorsOrLimitReadsTheWholeIndex(t *testing.T) {
	got := LabelNamesSQL(IndexQuery{})
	want := "SELECT DISTINCT arrayJoin(mapKeys(labels)) AS label FROM metric_series ORDER BY label"
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestLabelNamesSQLOnAClusterReadsTheDistributedTable(t *testing.T) {
	got := LabelNamesSQL(IndexQuery{Cluster: true})
	want := "SELECT DISTINCT arrayJoin(mapKeys(labels)) AS label FROM metric_series_dist ORDER BY label"
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestLabelValuesSQLExcludesAMissingLabelUnderORedSelectors(t *testing.T) {
	got := LabelValuesSQL("path", IndexQuery{
		Selectors: [][]*labels.Matcher{
			{matcher(labels.MatchEqual, "__name__", "http_requests_total"), matcher(labels.MatchNotEqual, "status", "500"),
				matcher(labels.MatchRegexp, "path", "/v1/.*")},
			{matcher(labels.MatchEqual, "__name__", "up")},
		},
		StartMs: &probeStart,
		EndMs:   &probeEnd,
	})
	want := "SELECT DISTINCT labels['path'] AS value FROM metric_series " +
		"WHERE last_seen >= fromUnixTimestamp64Milli(1790811000000) " +
		"AND first_seen <= fromUnixTimestamp64Milli(1790856000000) " +
		"AND ((name = 'http_requests_total' AND labels['status'] != '500' AND match(labels['path'], '^(?:/v1/.*)$')) " +
		"OR (name = 'up')) " +
		"AND labels['path'] != '' " +
		"ORDER BY value"
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestNameValuesSQLReadsTheNameColumn(t *testing.T) {
	got := LabelValuesSQL("__name__", IndexQuery{EndMs: &probeEnd, Limit: 1})
	want := "SELECT DISTINCT name AS value FROM metric_series " +
		"WHERE first_seen <= fromUnixTimestamp64Milli(1790856000000) " +
		"ORDER BY value LIMIT 2"
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestSeriesListSQLGroupsUnmergedRowsPerSeries(t *testing.T) {
	got := SeriesListSQL(IndexQuery{
		Selectors: [][]*labels.Matcher{{matcher(labels.MatchNotEqual, "status", "")}},
		Limit:     1,
	})
	want := "SELECT any(labels) AS label_set FROM metric_series " +
		"WHERE (labels['status'] != '') " +
		"GROUP BY name, fingerprint ORDER BY name, fingerprint LIMIT 2"
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestMetadataSQLReadsOneRowPerFamily(t *testing.T) {
	for _, tc := range []struct {
		metric  string
		limit   int
		cluster bool
		want    string
	}{
		{"", -1, false, "SELECT name, type, help, unit FROM metric_metadata FINAL ORDER BY name"},
		{"", 1, false, "SELECT name, type, help, unit FROM metric_metadata FINAL ORDER BY name LIMIT 1"},
		{"", 0, false, "SELECT name, type, help, unit FROM metric_metadata FINAL ORDER BY name LIMIT 0"},
		{"up", -1, true, "SELECT name, type, help, unit FROM metric_metadata_dist FINAL WHERE name = 'up' ORDER BY name"},
	} {
		if got := MetadataSQL(tc.metric, tc.limit, tc.cluster); got != tc.want {
			t.Errorf("got  %s\nwant %s", got, tc.want)
		}
	}
}

func TestExemplarsSQLReadsTheSelectedSeriesInClosedBounds(t *testing.T) {
	got := ExemplarsSQL(IndexQuery{
		Selectors: [][]*labels.Matcher{{matcher(labels.MatchEqual, "__name__", "up")}},
		StartMs:   &probeStart,
		EndMs:     &probeEnd,
	})
	want := "WITH fp AS (SELECT fingerprint, any(labels) AS label_set FROM metric_series " +
		"WHERE last_seen >= fromUnixTimestamp64Milli(1790811000000) " +
		"AND first_seen <= fromUnixTimestamp64Milli(1790856000000) " +
		"AND (name = 'up') GROUP BY fingerprint) " +
		"SELECT e.fingerprint, fp.label_set, e.timestamp, e.value, e.labels FROM metric_exemplars AS e " +
		"INNER JOIN fp ON e.fingerprint = fp.fingerprint " +
		"WHERE e.fingerprint IN (SELECT fingerprint FROM fp) " +
		"AND e.timestamp >= fromUnixTimestamp64Milli(1790812800000) " +
		"AND e.timestamp <= fromUnixTimestamp64Milli(1790856000000) " +
		"ORDER BY e.fingerprint, e.timestamp " +
		"LIMIT 1 BY e.fingerprint, e.timestamp, e.trace_id"
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestExemplarsSQLOnAClusterReadsLocalSeriesUnderTheDistributedTable(t *testing.T) {
	got := ExemplarsSQL(IndexQuery{
		Selectors: [][]*labels.Matcher{{matcher(labels.MatchEqual, "__name__", "up")}},
		Cluster:   true,
	})
	want := "WITH fp AS (SELECT fingerprint, any(labels) AS label_set FROM metric_series " +
		"WHERE (name = 'up') GROUP BY fingerprint) " +
		"SELECT e.fingerprint, fp.label_set, e.timestamp, e.value, e.labels FROM metric_exemplars_dist AS e " +
		"INNER JOIN fp ON e.fingerprint = fp.fingerprint " +
		"WHERE e.fingerprint IN (SELECT fingerprint FROM fp) " +
		"ORDER BY e.fingerprint, e.timestamp " +
		"LIMIT 1 BY e.fingerprint, e.timestamp, e.trace_id"
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestTheLargestLimitReadsEveryRow(t *testing.T) {
	got := LabelNamesSQL(IndexQuery{Limit: math.MaxInt})
	want := "SELECT DISTINCT arrayJoin(mapKeys(labels)) AS label FROM metric_series ORDER BY label"
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}
