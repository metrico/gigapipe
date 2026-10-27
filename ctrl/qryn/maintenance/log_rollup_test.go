package maintenance

import (
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/ctrl/qryn/sql"
)

const logRowsOnly = "\nWHERE samples.type != 2\nGROUP BY fingerprint, timestamp_ns, type"

func renderedLogStack(t *testing.T) []string {
	t.Helper()
	scripts, err := renderScripts(sql.LogScript, migrationEnv("cloki", "", false, 7, "", "", false, testTiers))
	if err != nil {
		t.Fatal(err)
	}
	return scripts
}

// lastLogRollupQueryChange returns the index of the last statement that sets
// the metrics_15s_mv query on an existing view.
func lastLogRollupQueryChange(t *testing.T, scripts []string) int {
	t.Helper()
	for i := len(scripts) - 1; i >= 0; i-- {
		if strings.HasPrefix(scripts[i], "ALTER TABLE cloki.metrics_15s_mv ") &&
			strings.Contains(scripts[i], "MODIFY QUERY") {
			return i
		}
	}
	t.Fatal("no statement modifies the metrics_15s_mv query")
	return -1
}

// TestLogRollupViewTakesOnlyLogRows checks the step every install ends with:
// a missing view is created and an existing one is given the log-row filter.
func TestLogRollupViewTakesOnlyLogRows(t *testing.T) {
	scripts := renderedLogStack(t)
	m := lastLogRollupQueryChange(t, scripts)
	if !strings.Contains(scripts[m], logRowsOnly) {
		t.Errorf("the metrics_15s_mv query change does not exclude metric rows:\n%s", scripts[m])
	}
	create := scripts[m-1]
	if !strings.HasPrefix(create, "CREATE MATERIALIZED VIEW IF NOT EXISTS cloki.metrics_15s_mv ") ||
		!strings.Contains(create, logRowsOnly) {
		t.Errorf("the statement before the query change does not create a filtered view:\n%s", create)
	}
	if m+1 >= len(scripts) || !strings.HasPrefix(scripts[m+1], "DROP TABLE IF EXISTS cloki.metrics_15s_mv_bak ") {
		t.Errorf("no backup view drop follows the query change")
	}
	for _, s := range scripts[m+1:] {
		if strings.Contains(s, "metrics_15s_mv ") && !strings.HasPrefix(s, "DROP TABLE IF EXISTS cloki.metrics_15s_mv_bak ") {
			t.Errorf("a later statement changes metrics_15s_mv again:\n%s", s)
		}
	}
}

// TestLogRollupStepCanBeRerun checks that the final step holds no statement
// that fails when repeated, whatever state an interrupted run left.
func TestLogRollupStepCanBeRerun(t *testing.T) {
	scripts := renderedLogStack(t)
	m := lastLogRollupQueryChange(t, scripts)
	for _, s := range scripts[m-1:] {
		if strings.HasPrefix(s, "RENAME ") {
			t.Errorf("the step renames a table:\n%s", s)
		}
	}
}
