package clickhouse_transpiler

import (
	"strings"
	"testing"
)

// A lone {} at limit 0 picks every trace.
func TestLoneSelectorAtLimitZeroHasNoLimit(t *testing.T) {
	ctx := testPlannerContext(3)
	ctx.Limit = 0
	for _, query := range []string{`{}`, `{} | select(.x)`} {
		s := renderPlan(t, planSearch(t, query).Ids[0].Statement, ctx)
		if strings.Contains(s, "LIMIT 0") {
			t.Errorf("%s: %s", query, s)
		}
	}
}
