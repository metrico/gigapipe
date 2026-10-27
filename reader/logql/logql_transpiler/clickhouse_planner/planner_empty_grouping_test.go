package clickhouse_planner

import (
	"strings"
	"testing"
	"time"
)

// `by ()` keeps no labels, which is exactly what an aggregation without any
// grouping clause does: the planner synthesises `by ()` for that case. The two
// spellings must therefore plan to the same SQL, on both the metrics_15s
// shortcut (a bare count_over_time) and the samples path (a line filter rules
// the shortcut out), and whether the clause comes before or after the
// aggregated expression.
func TestEmptyByPlansLikeNoGrouping(t *testing.T) {
	from := time.Unix(0, 1759000000_000000000)
	to := from.Add(time.Hour)
	step := time.Minute

	for _, tc := range []struct {
		name       string
		emptyBy    string
		noGrouping string
	}{
		{
			name:       "metrics15s shortcut, prefix",
			emptyBy:    `sum by () (count_over_time({app="x"}[5m]))`,
			noGrouping: `sum(count_over_time({app="x"}[5m]))`,
		},
		{
			name:       "metrics15s shortcut, suffix",
			emptyBy:    `sum (count_over_time({app="x"}[5m])) by ()`,
			noGrouping: `sum(count_over_time({app="x"}[5m]))`,
		},
		{
			name:       "samples path, prefix",
			emptyBy:    `max by () (count_over_time({app="x"} |= "error" [5m]))`,
			noGrouping: `max(count_over_time({app="x"} |= "error" [5m]))`,
		},
		{
			name:       "samples path, suffix",
			emptyBy:    `sum (rate({app="x"} |= "error" [5m])) by ()`,
			noGrouping: `sum(rate({app="x"} |= "error" [5m]))`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := planSQL(t, tc.emptyBy, from, to, step)
			want := planSQL(t, tc.noGrouping, from, to, step)
			if got != want {
				t.Errorf("%s\nplanned differently from\n%s\n--- by ():\n%s\n--- no grouping:\n%s",
					tc.emptyBy, tc.noGrouping, got, want)
			}
		})
	}
}

// `without ()` removes no labels, so every series keeps its full label set: the
// projection must neither collapse the labels to the empty map (that is
// `by ()`) nor filter them.
func TestEmptyWithoutKeepsAllLabels(t *testing.T) {
	from := time.Unix(0, 1759000000_000000000)
	to := from.Add(time.Hour)
	step := time.Minute

	for _, tc := range []struct {
		name     string
		empty    string
		emptyBy  string
		oneLabel string
	}{
		{
			name:     "metrics15s shortcut",
			empty:    `sum without () (count_over_time({app="x"}[5m]))`,
			emptyBy:  `sum by () (count_over_time({app="x"}[5m]))`,
			oneLabel: `sum without (pod) (count_over_time({app="x"}[5m]))`,
		},
		{
			name:     "samples path",
			empty:    `sum without () (count_over_time({app="x"} |= "error" [5m]))`,
			emptyBy:  `sum by () (count_over_time({app="x"} |= "error" [5m]))`,
			oneLabel: `sum without (pod) (count_over_time({app="x"} |= "error" [5m]))`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := planSQL(t, tc.empty, from, to, step)

			// Guards the assertions below: a named label must still be
			// filtered with mapFilter, or the absence check proves nothing.
			if filtered := planSQL(t, tc.oneLabel, from, to, step); !strings.Contains(filtered, "mapFilter(") {
				t.Fatalf("%s: expected a mapFilter projection, got:\n%s", tc.oneLabel, filtered)
			}

			if strings.Contains(got, "mapFilter(") {
				t.Errorf("%s filters labels, want them all kept:\n%s", tc.empty, got)
			}
			if strings.Contains(got, "CAST(map(), 'Map(String, String)')") {
				t.Errorf("%s collapses labels to the empty map, want them all kept:\n%s", tc.empty, got)
			}
			if got == planSQL(t, tc.emptyBy, from, to, step) {
				t.Errorf("%s planned the same as %s", tc.empty, tc.emptyBy)
			}
		})
	}
}
