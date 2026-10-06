package maintenance

import (
	"regexp"
	"slices"
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
		"metric_samples":     "TTL toDateTime(timestamp) + INTERVAL 3 DAY",
		"metric_exemplars":   "TTL toDateTime(timestamp) + INTERVAL 3 DAY",
		"metrics_5m":         "TTL toDateTime(bucket) + INTERVAL 14 DAY",
		"metrics_1h":         "TTL toDateTime(bucket) + INTERVAL 90 DAY",
		"metric_series":      "TTL toDateTime(last_seen) + INTERVAL 90 DAY",
		"metric_label_names": "TTL toDateTime(last_seen) + INTERVAL 90 DAY",
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

// selectList returns a view's SELECT items keyed by their alias.
func selectList(t *testing.T, view string) map[string]string {
	t.Helper()
	_, body, ok := strings.Cut(view, "\nSELECT\n")
	body, _, ok2 := strings.Cut(body, "\nFROM ")
	if !ok || !ok2 {
		t.Fatalf("no SELECT list in:\n%s", view)
	}
	items := map[string]string{}
	for _, item := range strings.Split(body, ",\n") {
		item = strings.TrimSpace(item)
		alias := item
		if i := strings.LastIndex(item, " AS "); i >= 0 {
			alias = item[i+len(" AS "):]
		}
		items[alias] = item
	}
	return items
}

var positiveStale = regexp.MustCompile(`\bstale\b`)

func TestStaleMarkersAreKeptOutOfEveryAggregateState(t *testing.T) {
	scripts := renderedMetricStack(t)
	for _, mv := range []string{"metrics_5m_mv", "metrics_1h_mv"} {
		s := statement(t, scripts, mv)
		if !strings.Contains(s, "reinterpretAsUInt64(value) = 0x7ff0000000000002 AS stale") ||
			!strings.Contains(s, "NOT stale AND NOT prev_stale AS paired") {
			t.Errorf("%s does not define stale and paired over the stale marker:\n%s", mv, s)
		}
		items := selectList(t, s)
		if len(items) != 13 {
			t.Errorf("%s selects %d columns, want 13", mv, len(items))
		}
		for alias, item := range items {
			if alias == "fingerprint" || alias == "bucket" {
				continue
			}
			takesStale := positiveStale.MatchString(strings.ReplaceAll(item, "NOT stale", ""))
			switch {
			case alias == "stale_at" && !takesStale:
				t.Errorf("%s: stale_at does not take the stale markers: %s", mv, item)
			case alias != "stale_at" && takesStale:
				t.Errorf("%s: %s takes the stale markers: %s", mv, alias, item)
			case alias != "stale_at" && !strings.Contains(item, "NOT stale") && !strings.Contains(item, "paired"):
				t.Errorf("%s: %s does not leave the stale markers out: %s", mv, alias, item)
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

func TestAggregateTiersKeepAMergeableVariance(t *testing.T) {
	scripts := renderedMetricStack(t)
	for table, mv := range map[string]string{"metrics_5m": "metrics_5m_mv", "metrics_1h": "metrics_1h_mv"} {
		if s := statement(t, scripts, table); !strings.Contains(s, "var         AggregateFunction(varPopStableIf, Float64, UInt8),") ||
			strings.Contains(s, "sum_sq") {
			t.Errorf("%s does not hold a variance state:\n%s", table, s)
		}
		if item := selectList(t, statement(t, scripts, mv))["var"]; item != "varPopStableIfState(value, NOT stale) AS var" {
			t.Errorf("%s: var = %q", mv, item)
		}
	}
}

func TestLabelNamesAreKeptPerLabelFromEverySeriesRow(t *testing.T) {
	scripts := renderedMetricStack(t)
	table := statement(t, scripts, "metric_label_names")
	for _, want := range []string{
		"  label      String,\n",
		"  first_seen SimpleAggregateFunction(min, DateTime64(3)),\n",
		"  last_seen  SimpleAggregateFunction(max, DateTime64(3))\n",
		"ENGINE = AggregatingMergeTree\nORDER BY label\n",
	} {
		if !strings.Contains(table, want) {
			t.Errorf("metric_label_names lacks %q:\n%s", want, table)
		}
	}
	const perLabel = "SELECT label, min(first_seen) AS first_seen, max(last_seen) AS last_seen\n" +
		"FROM cloki.metric_series\nARRAY JOIN mapKeys(labels) AS label\nGROUP BY label"
	mv := statement(t, scripts, "metric_label_names_mv")
	if !strings.Contains(mv, "TO cloki.metric_label_names AS\n"+perLabel) {
		t.Errorf("metric_label_names_mv does not feed one row per label from metric_series:\n%s", mv)
	}
	// The rows indexed before the view existed are copied once, after it.
	backfill := "INSERT INTO cloki.metric_label_names (label, first_seen, last_seen)\n" + perLabel
	i := slices.IndexFunc(scripts, func(s string) bool { return strings.TrimSuffix(s, ";") == backfill })
	if i < 0 || i < slices.Index(scripts, mv) {
		t.Errorf("no backfill after the view:\n%s", strings.Join(scripts, "\n\n"))
	}
}
