package service

import (
	"context"
	"database/sql/driver"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/plugins"
	"github.com/metrico/qryn/v5/reader/utils/fakeclickhouse"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

// pluginSeries selects series labels from plugin_series, naming the window it was asked for.
type pluginSeries struct{}

func (pluginSeries) GetMetricLabelsQuery(_ context.Context, _ *model.DataDatabasesMap,
	matchers []*labels.Matcher, from time.Time, to time.Time) string {
	return fmt.Sprintf("SELECT fingerprint, label_set FROM plugin_series WHERE %d %d %d",
		len(matchers), from.UnixMilli(), to.UnixMilli())
}

func registerPluginSeries(t *testing.T) {
	plugins.RegisterMetricLabelsGetterPlugin(pluginSeries{})
	t.Cleanup(func() { plugins.RegisterMetricLabelsGetterPlugin(nil) })
}

func TestSelectNamesRawSeriesFromTheLabelsGetterPlugin(t *testing.T) {
	registerPluginSeries(t)
	db := fakeclickhouse.New(func(query string) (fakeclickhouse.Result, error) {
		if strings.HasPrefix(query, "SELECT fingerprint, label_set FROM plugin_series WHERE 1 999 60000") {
			return fakeclickhouse.Result{Columns: []string{"fingerprint", "label_set"},
				Rows: [][]driver.Value{{uint64(1), map[string]string{"__name__": "x", "from": "plugin"}}}}, nil
		}
		if strings.Contains(query, "any(labels)") {
			return fakeclickhouse.Result{}, fmt.Errorf("series index read: %s", query)
		}
		return fakeclickhouse.Result{Columns: []string{"fingerprint", "timestamp", "value"},
			Rows: [][]driver.Value{{uint64(1), ms(15000), 1.0}}}, nil
	})
	got := selectSeries(t, db, parse(t, "x"), &storage.SelectHints{Start: 1000, End: 60000, Step: 15000},
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "x"))
	want := map[string][]point{`{__name__="x", from="plugin"}`: {{15000, math.Float64bits(1)}}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestSelectNamesSubstituteSeriesFromTheLabelsGetterPlugin(t *testing.T) {
	registerPluginSeries(t)
	db := substituteRows(
		[][]driver.Value{{uint64(7), map[string]string{"job": "a"}}},
		[]driver.Value{uint64(7), int64(60000), 1.0})
	selectSeries(t, db, rateSubstitute(t), &storage.SelectHints{Start: -239999, End: 300000, Step: 60000},
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "__metric_subst__1"))
	for _, q := range db.Queries() {
		if strings.Contains(q, "any(labels)") {
			t.Errorf("query reads labels from the series index: %s", q)
		}
		if !strings.Contains(q, "ARRAY JOIN") &&
			!strings.Contains(q, "FROM (SELECT fingerprint, label_set FROM plugin_series WHERE 1 -240000 300000)") {
			t.Errorf("labels read %q does not select from the plugin's query", q)
		}
	}
}
