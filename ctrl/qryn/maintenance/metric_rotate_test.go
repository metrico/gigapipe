package maintenance

import (
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

// alter returns the rendered ALTER statements of every metric rotation that
// touch table, and the TTL its rotation applies.
func alter(t *testing.T, table string, days []RotatePolicy) (string, []string) {
	t.Helper()
	for _, r := range metricRotations(testTiers) {
		if !slices.Contains(r.tables, table) {
			continue
		}
		ttl, stmts := r.statements("", days)
		var own []string
		for _, s := range stmts {
			if strings.HasPrefix(s, "ALTER TABLE "+table+" ") {
				own = append(own, s)
			}
		}
		return ttl, own
	}
	t.Fatalf("no metric rotation covers %s", table)
	return "", nil
}

func TestRotateAppliesTheRetentionTiersToTheMetricTables(t *testing.T) {
	for table, ttl := range map[string]string{
		"metric_samples":     "toDateTime(timestamp) + toIntervalDay(3)",
		"metric_exemplars":   "toDateTime(timestamp) + toIntervalDay(3)",
		"metrics_5m":         "toDateTime(bucket) + toIntervalDay(14)",
		"metrics_1h":         "toDateTime(bucket) + toIntervalDay(90)",
		"metric_series":      "toDateTime(last_seen) + toIntervalDay(90)",
		"metric_label_names": "toDateTime(last_seen) + toIntervalDay(90)",
	} {
		_, stmts := alter(t, table, nil)
		if !slices.ContainsFunc(stmts, func(s string) bool { return strings.HasSuffix(s, "MODIFY TTL "+ttl) }) {
			t.Errorf("%s: no MODIFY TTL %s in %q", table, ttl, stmts)
		}
	}
}

func TestRotateAppliesTheMoveRulesToTheMetricTables(t *testing.T) {
	days := []RotatePolicy{{TTL: 24 * time.Hour, MoveTo: "cold"}}
	_, stmts := alter(t, "metrics_5m", days)
	want := "MODIFY TTL toDateTime(bucket) + toIntervalSecond(86400) TO DISK 'cold', toDateTime(bucket) + toIntervalDay(14)"
	if !slices.ContainsFunc(stmts, func(s string) bool { return strings.HasSuffix(s, want) }) {
		t.Errorf("metrics_5m: no %q in %q", want, stmts)
	}
}

func TestSeriesIndexRowsExpireOneByOne(t *testing.T) {
	for _, table := range []string{"metric_series", "metric_label_names"} {
		_, stmts := alter(t, table, nil)
		for _, s := range stmts {
			if strings.Contains(s, "ttl_only_drop_parts") {
				t.Errorf("%s is set to drop whole parts: %s", table, s)
			}
		}
	}
	_, stmts := alter(t, "metric_samples", nil)
	if !slices.ContainsFunc(stmts, func(s string) bool { return strings.Contains(s, "ttl_only_drop_parts = 1") }) {
		t.Errorf("metric_samples is not set to drop whole parts: %q", stmts)
	}
}

var createdTable = regexp.MustCompile(`^CREATE TABLE IF NOT EXISTS cloki\.(\w+) `)

func TestStoragePolicyCoversEveryStoringMetricTable(t *testing.T) {
	var want []string
	for _, s := range renderedMetricStack(t) {
		m := createdTable.FindStringSubmatch(s)
		if m != nil && !strings.Contains(s, "ENGINE = Null") {
			want = append(want, m[1])
		}
	}
	slices.Sort(want)
	got := slices.Sorted(slices.Values(metricStoringTables))
	if len(want) == 0 || !slices.Equal(got, want) {
		t.Errorf("storing metric tables = %v, want %v", got, want)
	}
	for _, r := range metricRotations(testTiers) {
		for _, table := range r.tables {
			if !slices.Contains(metricStoringTables, table) {
				t.Errorf("%s has a TTL but no storage policy", table)
			}
		}
	}
}
