package maintenance

import (
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/ctrl/qryn/sql"
)

// TestLogRollupViewTakesOnlyLogRows checks the metrics_15s_mv definition every
// install ends with, rendered with no setting beyond the migration env.
func TestLogRollupViewTakesOnlyLogRows(t *testing.T) {
	scripts, err := renderScripts(sql.LogScript, migrationEnv("cloki", "", false, 7, "", "", false, testTiers))
	if err != nil {
		t.Fatal(err)
	}
	last := ""
	for _, s := range scripts {
		if strings.HasPrefix(s, "CREATE MATERIALIZED VIEW IF NOT EXISTS cloki.metrics_15s_mv ") {
			last = s
		}
	}
	if last == "" {
		t.Fatal("no statement creates metrics_15s_mv")
	}
	if !strings.Contains(last, "\nWHERE samples.type != 2\nGROUP BY fingerprint, timestamp_ns, type") {
		t.Errorf("the final metrics_15s_mv does not exclude metric rows:\n%s", last)
	}
}
