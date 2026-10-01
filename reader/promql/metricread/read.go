package metricread

import (
	"fmt"

	"github.com/metrico/qryn/v5/reader/utils/tables"
	"github.com/prometheus/prometheus/model/labels"
)

// Window is the read interval (FromMs, ToMs] in unix milliseconds.
type Window struct {
	FromMs int64
	ToMs   int64
}

// seriesIndexLagMs is how far a live series' last_seen may trail its newest sample: the
// writer emits a series row once per 30-minute fingerprint cache period.
const seriesIndexLagMs = 30 * 60 * 1000

// SeriesSQL selects the fingerprint and label set of every series matching the selectors
// that may have samples in w. Rows: fingerprint UInt64, label_set Map(String, String).
func SeriesSQL(w Window, selectors ...[]*labels.Matcher) string {
	return fmt.Sprintf("SELECT fingerprint, any(labels) AS label_set FROM %s "+
		"WHERE (%s) "+
		"AND last_seen >= fromUnixTimestamp64Milli(%d) "+
		"AND first_seen <= fromUnixTimestamp64Milli(%d) "+
		"GROUP BY fingerprint",
		tables.GetTableName("metric_series"), SelectorPredicate(selectors...), w.FromMs-seriesIndexLagMs, w.ToMs)
}

// RawSamplesSQL selects the raw samples in w of the series SeriesSQL selects, one row per
// (fingerprint, timestamp). Rows: fingerprint UInt64, timestamp DateTime64(3), value Float64.
func RawSamplesSQL(w Window, selectors ...[]*labels.Matcher) string {
	return fmt.Sprintf("WITH fp AS (%s) "+
		"SELECT fingerprint, timestamp, value FROM %s "+
		"WHERE fingerprint IN (SELECT fingerprint FROM fp) "+
		"AND timestamp > fromUnixTimestamp64Milli(%d) "+
		"AND timestamp <= fromUnixTimestamp64Milli(%d) "+
		"ORDER BY fingerprint, timestamp "+
		"LIMIT 1 BY fingerprint, timestamp",
		SeriesSQL(w, selectors...), tables.GetTableName("metric_samples"), w.FromMs, w.ToMs)
}
