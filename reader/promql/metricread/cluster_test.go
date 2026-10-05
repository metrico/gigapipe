package metricread

import (
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/reader/model"
	"github.com/prometheus/prometheus/model/labels"
)

var fromTable = regexp.MustCompile(`FROM (\w+)`)

// tablesRead lists the tables sql reads, in order.
func tablesRead(sql string) []string {
	var res []string
	for _, m := range fromTable.FindAllStringSubmatch(sql, -1) {
		if m[1] != "fp" && m[1] != "rows" {
			res = append(res, m[1])
		}
	}
	return res
}

// localSeriesFilter is the per-shard series filter: the local series index under a
// distributed read.
const localSeriesFilter = "fingerprint IN (SELECT fingerprint FROM metric_series WHERE (name = 'x') AND "

func TestSamplesOnAClusterAreFilteredByEachShardsLocalSeries(t *testing.T) {
	cw := probe
	cw.Cluster = true
	pushdown := func(fn string, tier Tier) string {
		return PushdownSQL(Pushdown{Grid: probeGrid, Func: fn, RangeMs: 300000, Matchers: probeSelector(),
			Tier: tier, Cluster: true})
	}
	aggregated := PushdownSQL(Pushdown{Grid: probeGrid, Func: "rate", RangeMs: 300000, Matchers: probeSelector(),
		Aggregation: &Aggregation{Op: "sum"}, Cluster: true})
	for name, tc := range map[string]struct {
		sql  string
		want []string
	}{
		"raw samples":      {RawSamplesSQL(cw, probeSelector()), []string{"metric_samples_dist", "metric_series"}},
		"tier samples":     {TierSamplesSQL(cw, Tier5m, probeSelector()), []string{"metrics_5m_dist", "metric_series"}},
		"raw pushdown":     {pushdown("rate", RawTier), []string{"metric_samples_dist", "metric_series"}},
		"raw aggregation":  {aggregated, []string{"metric_series_dist", "metric_samples_dist", "metric_series"}},
		"tier pushdown":    {pushdown("rate", Tier5m), []string{"metrics_5m_dist", "metric_series"}},
		"tier instant":     {pushdown("", Tier1h), []string{"metrics_1h_dist", "metric_series"}},
		"unaligned stamps": {PushdownSQL(Pushdown{Grid: Grid{StartMs: 1767225660000, EndMs: 1767226260000, StepMs: 60000}, Func: "rate", RangeMs: 300000, Matchers: probeSelector(), Tier: Tier5m, Cluster: true}), []string{"metrics_5m_dist", "metric_series"}},
	} {
		if got := tablesRead(tc.sql); !slices.Equal(got, tc.want) {
			t.Errorf("%s reads %v, want %v:\n%s", name, got, tc.want, tc.sql)
		}
		if !strings.Contains(tc.sql, localSeriesFilter) {
			t.Errorf("%s does not filter by the local series:\n%s", name, tc.sql)
		}
		if strings.Contains(tc.sql, "GLOBAL") {
			t.Errorf("%s ships a set across shards:\n%s", name, tc.sql)
		}
	}
}

func TestSeriesSelectionOnAClusterReadsTheDistributedIndex(t *testing.T) {
	cw := probe
	cw.Cluster = true
	if got := tablesRead(SeriesSQL(cw, probeSelector())); !slices.Equal(got, []string{"metric_series_dist"}) {
		t.Errorf("series selection reads %v", got)
	}
	q := model.MetricIndexQuery{Selectors: [][]*labels.Matcher{probeSelector()}, Cluster: true}
	for name, sql := range map[string]string{
		"labels":   LabelNamesSQL(q),
		"values":   LabelValuesSQL("job", q),
		"__name__": LabelValuesSQL(labels.MetricName, q),
		"series":   SeriesListSQL(q),
		"pushdown labels": PushdownLabelsSQL(Pushdown{Grid: probeGrid, Func: "rate", RangeMs: 300000,
			Matchers: probeSelector(), Cluster: true}),
		"metadata": MetadataSQL("x", -1, true),
	} {
		got := tablesRead(sql)
		if len(got) != 1 || !strings.HasSuffix(got[0], "_dist") {
			t.Errorf("%s reads %v:\n%s", name, got, sql)
		}
	}
}

func TestSamplesOnASingleNodeReadTheLocalTables(t *testing.T) {
	got := tablesRead(PushdownSQL(Pushdown{Grid: probeGrid, Func: "rate", RangeMs: 300000, Matchers: probeSelector()}))
	if want := []string{"metric_samples", "metric_series"}; !slices.Equal(got, want) {
		t.Errorf("reads %v, want %v", got, want)
	}
}

func TestReadsOnAClusterNameTheDistributedTables(t *testing.T) {
	cw := probe
	cw.Cluster = true
	x := []*labels.Matcher{matcher(labels.MatchEqual, "__name__", "x")}
	q := model.MetricIndexQuery{Selectors: [][]*labels.Matcher{x}, Cluster: true, Limit: 1}
	for name, tc := range map[string]struct{ got, want string }{
		"series": {SeriesSQL(cw, x),
			"SELECT fingerprint, any(labels) AS label_set FROM metric_series_dist " +
				"WHERE (name = 'x') " +
				"AND last_seen >= fromUnixTimestamp64Milli(1767223800000) " +
				"AND first_seen <= fromUnixTimestamp64Milli(1767226200000) " +
				"GROUP BY fingerprint"},
		"raw samples": {RawSamplesSQL(cw, x),
			"SELECT fingerprint, timestamp, value FROM metric_samples_dist FINAL " +
				"WHERE " + localSeries(1767223800000, 1767226200000) + " " +
				"AND timestamp > fromUnixTimestamp64Milli(1767225600000) " +
				"AND timestamp <= fromUnixTimestamp64Milli(1767226200000) " +
				"ORDER BY fingerprint, timestamp " +
				"SETTINGS do_not_merge_across_partitions_select_final = 1"},
		"label values": {LabelValuesSQL("job", q),
			"SELECT DISTINCT labels['job'] AS value FROM metric_series_dist " +
				"WHERE (name = 'x') AND labels['job'] != '' ORDER BY value LIMIT 2"},
		"name values": {LabelValuesSQL(labels.MetricName, q),
			"SELECT DISTINCT name AS value FROM metric_series_dist WHERE (name = 'x') ORDER BY value LIMIT 2"},
		"series list": {SeriesListSQL(q),
			"SELECT any(labels) AS label_set FROM metric_series_dist WHERE (name = 'x') " +
				"GROUP BY name, fingerprint ORDER BY name, fingerprint LIMIT 2"},
		"metadata": {MetadataSQL("", 1, true),
			"SELECT name, type, help, unit FROM metric_metadata_dist FINAL ORDER BY name LIMIT 1"},
	} {
		if tc.got != tc.want {
			t.Errorf("%s:\ngot  %s\nwant %s", name, tc.got, tc.want)
		}
	}
}
