package metricread

import (
	"fmt"

	"github.com/metrico/qryn/v5/reader/utils/tables"
	"github.com/metrico/qryn/v5/shared/metricindex"
	"github.com/prometheus/prometheus/model/labels"
)

// Window is the read interval (FromMs, ToMs] in unix milliseconds.
type Window struct {
	FromMs int64
	ToMs   int64
	// Cluster reads the distributed tables, each shard filtering its samples by its own series.
	Cluster bool
	// Series, when set, names the series in place of the series index.
	Series SeriesSource
}

// SeriesSource selects the fingerprint and label set of every series matching matchers that
// may have samples in w. Rows: fingerprint UInt64, label_set Map(String, String).
type SeriesSource func(w Window, matchers []*labels.Matcher) string

// SeriesSQL selects the fingerprint and label set of every series matching matchers that may
// have samples in w, from w.Series when set. Rows: fingerprint UInt64, label_set Map(String, String).
func SeriesSQL(w Window, matchers []*labels.Matcher) string {
	if w.Series != nil {
		return w.Series(w, matchers)
	}
	return fmt.Sprintf("SELECT fingerprint, any(labels) AS label_set FROM %s WHERE %s GROUP BY fingerprint",
		w.table("metric_series"), seriesWhere(w, [][]*labels.Matcher{matchers}))
}

// seriesIn keeps the fingerprints of the series matching the selectors in w, read from the local
// series index: on a cluster each shard holds the series rows of its own samples.
func seriesIn(w Window, selectors ...[]*labels.Matcher) string {
	return fmt.Sprintf("fingerprint IN (SELECT fingerprint FROM %s WHERE %s)",
		tables.GetTableName("metric_series"), seriesWhere(w, selectors))
}

func seriesWhere(w Window, selectors [][]*labels.Matcher) string {
	return fmt.Sprintf("(%s) "+
		"AND last_seen >= fromUnixTimestamp64Milli(%d) "+
		"AND first_seen <= fromUnixTimestamp64Milli(%d)",
		SelectorPredicate(selectors...), w.FromMs-metricindex.SeriesIndexLag.Milliseconds(), w.ToMs)
}

// table names the table w reads: its distributed wrapper on a cluster.
func (w Window) table(name string) string {
	if w.Cluster {
		return tables.GetTableName(name + "_dist")
	}
	return tables.GetTableName(name)
}

// RawSamplesSQL selects the raw samples in w of the series SeriesSQL selects, one row per
// (fingerprint, timestamp), the last written. Rows: fingerprint UInt64, timestamp DateTime64(3),
// value Float64, ordered by both.
func RawSamplesSQL(w Window, selectors ...[]*labels.Matcher) string {
	return fmt.Sprintf("SELECT fingerprint, timestamp, value FROM %s FINAL "+
		"WHERE %s "+
		"AND timestamp > fromUnixTimestamp64Milli(%d) "+
		"AND timestamp <= fromUnixTimestamp64Milli(%d) "+
		"ORDER BY fingerprint, timestamp "+
		"SETTINGS %s",
		w.table("metric_samples"), seriesIn(w, selectors...), w.FromMs, w.ToMs, finalSettings)
}
