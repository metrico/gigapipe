package metricread

import (
	"regexp"
	"slices"
	"strings"
	"testing"

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
	for name, tc := range map[string]struct {
		sql  string
		want []string
	}{
		"raw samples":      {RawSamplesSQL(cw, probeSelector()), []string{"metric_samples_dist", "metric_series"}},
		"tier samples":     {TierSamplesSQL(cw, Tier5m, probeSelector()), []string{"metrics_5m_dist", "metric_series"}},
		"raw pushdown":     {pushdown("rate", RawTier), []string{"metric_series_dist", "metric_samples_dist", "metric_series"}},
		"tier pushdown":    {pushdown("rate", Tier5m), []string{"metric_series_dist", "metrics_5m_dist", "metric_series"}},
		"tier instant":     {pushdown("", Tier1h), []string{"metric_series_dist", "metrics_1h_dist", "metric_series"}},
		"unaligned stamps": {PushdownSQL(Pushdown{Grid: Grid{StartMs: 1767225660000, EndMs: 1767226260000, StepMs: 60000}, Func: "rate", RangeMs: 300000, Matchers: probeSelector(), Tier: Tier5m, Cluster: true}), []string{"metric_series_dist", "metrics_5m_dist", "metric_series"}},
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
	q := IndexQuery{Selectors: [][]*labels.Matcher{probeSelector()}, Cluster: true}
	for name, sql := range map[string]string{
		"labels":   LabelNamesSQL(q),
		"values":   LabelValuesSQL("job", q),
		"__name__": LabelValuesSQL(labels.MetricName, q),
		"series":   SeriesListSQL(q),
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
	if want := []string{"metric_series", "metric_samples", "metric_series"}; !slices.Equal(got, want) {
		t.Errorf("reads %v, want %v", got, want)
	}
}
