// Package metricindex holds what the writer and the reader agree on about metric_series.
package metricindex

import "time"

// SeriesIndexLag is how often the writer re-emits a live series' metric_series row, and so
// how far that row's last_seen may trail the series' newest sample.
const SeriesIndexLag = 30 * time.Minute
