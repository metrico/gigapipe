package clickhouse_planner

import (
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
)

func shortcutFrom(t *testing.T, versionInfo dbversion.VersionInfo) string {
	t.Helper()
	ctx := &shared.PlannerContext{
		From:                    time.Unix(1_700_000_000, 0),
		To:                      time.Unix(1_700_003_600, 0),
		VersionInfo:             versionInfo,
		Metrics15sDistTableName: "metrics_15s_dist",
		SamplesDistTableName:    "samples_v3_dist",
	}
	sel, err := (&Metrics15ShortcutPlanner{Function: "rate", Duration: time.Minute}).Process(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, err := sel.String(newCtx())
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// The route depends only on the rollup table's capability; a settings row named
// metrics_15s has no effect.
func TestLogRollupServesRateWhateverTheSettings(t *testing.T) {
	for name, v := range map[string]dbversion.VersionInfo{
		"no settings row":     {dbversion.CapMetrics15s: 0},
		"far-future settings": {dbversion.CapMetrics15s: 0, "metrics_15s": 4102444800},
		"recent settings row": {dbversion.CapMetrics15s: 0, "metrics_15s": 1_700_001_000},
	} {
		if got := shortcutFrom(t, v); !strings.Contains(got, "FROM metrics_15s_dist") ||
			!strings.Contains(got, "countMerge(count)") {
			t.Errorf("%s: rate does not read the log rollup:\n%s", name, got)
		}
	}
}

func TestRateReadsRawLogRowsWithoutTheRollupTable(t *testing.T) {
	got := shortcutFrom(t, dbversion.VersionInfo{})
	if !strings.Contains(got, "FROM samples_v3_dist") || strings.Contains(got, "countMerge") {
		t.Errorf("rate does not read raw log rows:\n%s", got)
	}
}
