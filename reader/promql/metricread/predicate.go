// Package metricread builds the SQL the PromQL engine reads the metric stack with.
package metricread

import (
	"strings"

	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/prometheus/prometheus/model/labels"
)

// SelectorPredicate renders selectors as one boolean over a metric_series row: the matchers
// of a selector are ANDed and several selectors ORed.
func SelectorPredicate(selectors ...[]*labels.Matcher) string {
	if len(selectors) == 1 {
		return selectorPredicate(selectors[0])
	}
	parts := make([]string, len(selectors))
	for i, s := range selectors {
		parts[i] = "(" + selectorPredicate(s) + ")"
	}
	return strings.Join(parts, " OR ")
}

func selectorPredicate(matchers []*labels.Matcher) string {
	parts := make([]string, len(matchers))
	for i, m := range matchers {
		parts[i] = matcherPredicate(m)
	}
	return strings.Join(parts, " AND ")
}

// matcherPredicate reads __name__ from the name column and every other label from the
// labels map, where a missing key reads as the empty string.
func matcherPredicate(m *labels.Matcher) string {
	col := "name"
	if m.Name != labels.MetricName {
		col = "labels[" + quote(m.Name) + "]"
	}
	switch m.Type {
	case labels.MatchNotEqual:
		return col + " != " + quote(m.Value)
	case labels.MatchRegexp:
		return "match(" + col + ", " + quote("^(?:"+m.Value+")$") + ")"
	case labels.MatchNotRegexp:
		return "NOT match(" + col + ", " + quote("^(?:"+m.Value+")$") + ")"
	default:
		return col + " = " + quote(m.Value)
	}
}

func quote(s string) string {
	str, _ := sql.NewStringVal(s).String(nil)
	return str
}
