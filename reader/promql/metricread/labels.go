package metricread

import (
	"fmt"
	"math"
	"strings"

	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/shared/metricindex"
	"github.com/prometheus/prometheus/model/labels"
)

// LabelNamesSQL selects the label names of the series q picks, sorted: from the label names
// table without a selector, from the series index with one. Rows: label String.
func LabelNamesSQL(q model.MetricIndexQuery) string {
	if len(q.Selectors) > 0 {
		return "SELECT DISTINCT arrayJoin(mapKeys(labels)) AS label FROM " + table("metric_series", q.Cluster) +
			indexWhere(q) + " ORDER BY label" + indexLimit(q)
	}
	var conds []string
	if q.StartMs != nil {
		conds = append(conds, fmt.Sprintf("max(last_seen) >= fromUnixTimestamp64Milli(%d)",
			*q.StartMs-metricindex.SeriesIndexLag.Milliseconds()))
	}
	if q.EndMs != nil {
		conds = append(conds, fmt.Sprintf("min(first_seen) <= fromUnixTimestamp64Milli(%d)", *q.EndMs))
	}
	having := ""
	if len(conds) > 0 {
		having = " HAVING " + strings.Join(conds, " AND ")
	}
	sql := "SELECT label FROM " + table("metric_label_names", q.Cluster) + " GROUP BY label" + having +
		" ORDER BY label" + indexLimit(q)
	if q.Cluster {
		// A label has rows on every shard holding one of its series: the initiator merges them.
		sql += " SETTINGS optimize_distributed_group_by_sharding_key = 0"
	}
	return sql
}

// LabelValuesSQL selects the non-empty values of label name over the series q picks, sorted;
// __name__ reads the name column. Rows: value String.
func LabelValuesSQL(name string, q model.MetricIndexQuery) string {
	if name == labels.MetricName {
		return "SELECT DISTINCT name AS value FROM " + table("metric_series", q.Cluster) + indexWhere(q) +
			" ORDER BY value" + indexLimit(q)
	}
	col := "labels[" + quote(name) + "]"
	return "SELECT DISTINCT " + col + " AS value FROM " + table("metric_series", q.Cluster) +
		indexWhere(q, col+" != ''") + " ORDER BY value" + indexLimit(q)
}

// SeriesListSQL selects one label set per series q picks, ordered by name and fingerprint.
// Rows: label_set Map(String, String).
func SeriesListSQL(q model.MetricIndexQuery) string {
	return "SELECT any(labels) AS label_set FROM " + table("metric_series", q.Cluster) + indexWhere(q) +
		" GROUP BY name, fingerprint ORDER BY name, fingerprint" + indexLimit(q)
}

// MetadataSQL selects the latest metadata of every family, or of metric alone when set, up
// to limit families; a negative limit reads every family. Rows: name, type, help, unit String.
func MetadataSQL(metric string, limit int, cluster bool) string {
	q := "SELECT name, type, help, unit FROM " + table("metric_metadata", cluster) + " FINAL"
	if metric != "" {
		q += " WHERE name = " + quote(metric)
	}
	q += " ORDER BY name"
	if limit >= 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}
	return q
}

// ExemplarsSQL selects the exemplars in [StartMs, EndMs] of the series q picks, each with its
// series' label set, ordered by fingerprint and timestamp. The series are read from the local
// metric_series even on a cluster. Rows: fingerprint UInt64, label_set Map(String, String),
// timestamp DateTime64(3), value Float64, labels String.
func ExemplarsSQL(q model.MetricIndexQuery) string {
	conds := []string{"e.fingerprint IN (SELECT fingerprint FROM fp)"}
	if q.StartMs != nil {
		conds = append(conds, fmt.Sprintf("e.timestamp >= fromUnixTimestamp64Milli(%d)", *q.StartMs))
	}
	if q.EndMs != nil {
		conds = append(conds, fmt.Sprintf("e.timestamp <= fromUnixTimestamp64Milli(%d)", *q.EndMs))
	}
	return "WITH fp AS (SELECT fingerprint, any(labels) AS label_set FROM " + table("metric_series", false) +
		indexWhere(q) + " GROUP BY fingerprint) " +
		"SELECT e.fingerprint, fp.label_set, e.timestamp, e.value, e.labels " +
		"FROM " + table("metric_exemplars", q.Cluster) + " AS e " +
		"INNER JOIN fp ON e.fingerprint = fp.fingerprint " +
		"WHERE " + strings.Join(conds, " AND ") +
		" ORDER BY e.fingerprint, e.timestamp LIMIT 1 BY e.fingerprint, e.timestamp, e.trace_id"
}

// indexWhere tests the series' lifetime for overlap with [StartMs, EndMs], allowing last_seen
// to trail by the series-index lag, and applies the selectors.
func indexWhere(q model.MetricIndexQuery, extra ...string) string {
	var conds []string
	if q.StartMs != nil {
		conds = append(conds, fmt.Sprintf("last_seen >= fromUnixTimestamp64Milli(%d)",
			*q.StartMs-metricindex.SeriesIndexLag.Milliseconds()))
	}
	if q.EndMs != nil {
		conds = append(conds, fmt.Sprintf("first_seen <= fromUnixTimestamp64Milli(%d)", *q.EndMs))
	}
	if len(q.Selectors) > 0 {
		conds = append(conds, "("+SelectorPredicate(q.Selectors...)+")")
	}
	conds = append(conds, extra...)
	if len(conds) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(conds, " AND ")
}

func indexLimit(q model.MetricIndexQuery) string {
	if q.Limit <= 0 || q.Limit == math.MaxInt {
		return ""
	}
	return fmt.Sprintf(" LIMIT %d", q.Limit+1)
}
