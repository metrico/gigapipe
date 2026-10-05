package maintenance

import (
	"os"
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/ctrl/qryn/sql"
)

func golden(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestTierViewsRenderAsPinned(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"single":  migrationEnv("cloki", "", false, 7, "", "", false),
		"cluster": migrationEnv("cloki", "c1", true, 7, "", "", true),
	} {
		scripts, err := renderScripts(sql.MetricsScript, env)
		if err != nil {
			t.Fatal(err)
		}
		if len(scripts) != 14 {
			t.Errorf("%s: metrics.sql renders %d statements, want 14", name, len(scripts))
		}
		for _, mv := range []string{"metrics_5m_mv", "metrics_1h_mv"} {
			if got, want := statement(t, scripts, mv), golden(t, mv+"."+name+".sql"); got != want {
				t.Errorf("%s %s rendered:\n%s\nwant:\n%s", name, mv, got, want)
			}
		}
	}
}

func TestTierAggregationRendersTheViewBody(t *testing.T) {
	for _, tier := range metricTiers {
		view := golden(t, tier.Table+"_mv.single.sql")
		_, body, _ := strings.Cut(view, " AS\n")
		if got := tierAggregation(tier, "cloki.metric_samples_in"); got != body {
			t.Errorf("%s aggregation:\n%s\nwant the view body:\n%s", tier.Table, got, body)
		}
	}
}

func TestTierAggregationReadsTheGivenInput(t *testing.T) {
	widths := map[string]string{"metrics_5m": "INTERVAL 5 MINUTE", "metrics_1h": "INTERVAL 1 HOUR"}
	input := "(SELECT fingerprint, timestamp, value, prev_timestamp, prev_value, 1 AS aggregate FROM cloki.metric_samples FINAL) AS src"
	for _, tier := range metricTiers {
		got := tierAggregation(tier, input)
		if !strings.HasSuffix(got, "\nFROM "+input+"\nWHERE aggregate\nGROUP BY fingerprint, bucket") {
			t.Errorf("%s aggregation does not read the input:\n%s", tier.Table, got)
		}
		if n := strings.Count(got, widths[tier.Table]); n != 4 {
			t.Errorf("%s aggregation has %d %q, want 4:\n%s", tier.Table, n, widths[tier.Table], got)
		}
		if strings.Contains(got, "metric_samples_in") || strings.Contains(got, "{{") {
			t.Errorf("%s aggregation leaks another input or a template token:\n%s", tier.Table, got)
		}
	}
}
