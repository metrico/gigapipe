package maintenance

import (
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/ctrl/qryn/sql"
	"github.com/metrico/qryn/v5/shared/metricretention"
)

var testTiers = metricretention.Tiers{RawDays: 3, FiveMinuteDays: 14, HourDays: 90}

func renderedMetricStack(t *testing.T) []string {
	t.Helper()
	env := migrationEnv("cloki", "", false, 7, "", "", false, testTiers)
	scripts, err := renderScripts(sql.MetricsScript, env)
	if err != nil {
		t.Fatal(err)
	}
	return scripts
}

// statement returns the one rendered statement that creates object.
func statement(t *testing.T, scripts []string, object string) string {
	t.Helper()
	var found []string
	for _, s := range scripts {
		if strings.HasPrefix(s, "CREATE ") && strings.Contains(s, "IF NOT EXISTS cloki."+object+" ") {
			found = append(found, s)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%d statements create %s, want 1", len(found), object)
	}
	return found[0]
}

func TestRetentionTiersSetTheMetricTablesTTLs(t *testing.T) {
	scripts := renderedMetricStack(t)
	for object, ttl := range map[string]string{
		"metric_samples":   "TTL toDateTime(timestamp) + INTERVAL 3 DAY",
		"metric_exemplars": "TTL toDateTime(timestamp) + INTERVAL 3 DAY",
		"metrics_5m":       "TTL toDateTime(bucket) + INTERVAL 14 DAY",
		"metrics_1h":       "TTL toDateTime(bucket) + INTERVAL 90 DAY",
		"metric_series":    "TTL toDateTime(last_seen) + INTERVAL 90 DAY",
	} {
		if s := statement(t, scripts, object); !strings.Contains(s, ttl) {
			t.Errorf("%s lacks %q:\n%s", object, ttl, s)
		}
	}
}

func TestRawTierTakesEveryStagedRow(t *testing.T) {
	mv := statement(t, renderedMetricStack(t), "metric_samples_mv")
	if !strings.Contains(mv, "TO cloki.metric_samples AS") {
		t.Errorf("metric_samples_mv does not feed metric_samples:\n%s", mv)
	}
	if strings.Contains(mv, "WHERE") {
		t.Errorf("metric_samples_mv filters staged rows:\n%s", mv)
	}
}

func TestAggregateTiersTakeOnlyRowsMarkedAggregate(t *testing.T) {
	scripts := renderedMetricStack(t)
	for _, mv := range []string{"metrics_5m_mv", "metrics_1h_mv"} {
		s := statement(t, scripts, mv)
		if !strings.Contains(s, "FROM cloki.metric_samples_in\nWHERE aggregate\nGROUP BY fingerprint, bucket") {
			t.Errorf("%s does not restrict itself to aggregate rows:\n%s", mv, s)
		}
	}
}

func TestStaleMarkersAreKeptOutOfEveryAggregateState(t *testing.T) {
	scripts := renderedMetricStack(t)
	want := []string{
		"reinterpretAsUInt64(value) = 0x7ff0000000000002 AS stale",
		"reinterpretAsUInt64(prev_value) = 0x7ff0000000000002 AS prev_stale",
		"AND NOT stale AND NOT prev_stale AS paired",
		"minIfState((timestamp, value), NOT stale) AS first",
		"maxIfState((timestamp, value), NOT stale) AS last",
		"countIf(NOT stale) AS count",
		"sumIf(value, NOT stale) AS sum",
		"sumIf(value * value, NOT stale) AS sum_sq",
		"minIfState(value, NOT stale) AS min",
		"maxIfState(value, NOT stale) AS max",
		"countIf(paired AND value < prev_value) AS resets",
		"sumIf(prev_value, paired AND value < prev_value) AS reset_drop",
		"countIf(paired AND value != prev_value AND NOT (isNaN(value) AND isNaN(prev_value))) AS changes",
		"maxIf(timestamp, stale) AS stale_at",
	}
	for _, mv := range []string{"metrics_5m_mv", "metrics_1h_mv"} {
		s := statement(t, scripts, mv)
		for _, w := range want {
			if !strings.Contains(s, w) {
				t.Errorf("%s lacks %q", mv, w)
			}
		}
	}
}

func TestAggregateBucketsAreKeyedByTheirEnd(t *testing.T) {
	scripts := renderedMetricStack(t)
	for mv, width := range map[string]string{"metrics_5m_mv": "5 MINUTE", "metrics_1h_mv": "1 HOUR"} {
		s := statement(t, scripts, mv)
		end := "toStartOfInterval(timestamp - INTERVAL 1 MILLISECOND, INTERVAL " + width + ") + INTERVAL " +
			width + " AS bucket_end"
		if !strings.Contains(s, end) || !strings.Contains(s, "toDateTime64(bucket_end, 3) AS bucket") {
			t.Errorf("%s does not key buckets by their end at %s:\n%s", mv, width, s)
		}
	}
}
