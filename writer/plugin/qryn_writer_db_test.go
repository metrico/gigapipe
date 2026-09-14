package plugin

import (
	"slices"
	"testing"
)

// A wrong table name here panics a healthy install at startup (healthCheck
// calls panic on a failed probe), so the probed set for each layout is worth
// pinning down without a live ClickHouse connection.
func TestHealthCheckTables(t *testing.T) {
	for _, tc := range []struct {
		name           string
		splitBySignal  bool
		wantTables     []string
		wantDistTables []string
	}{
		{
			name:          "shared samples table",
			splitBySignal: false,
			wantTables: []string{
				"time_series", "settings",
				"tempo_traces", "tempo_traces_attrs_gin",
				"samples_v3",
			},
			wantDistTables: []string{
				"time_series_dist",
				"tempo_traces_dist", "tempo_traces_attrs_gin_dist",
				"samples_v3_dist",
			},
		},
		{
			name:          "split by signal",
			splitBySignal: true,
			wantTables: []string{
				"time_series", "settings",
				"tempo_traces", "tempo_traces_attrs_gin",
				"samples_logs", "samples_metrics",
			},
			wantDistTables: []string{
				"time_series_dist",
				"tempo_traces_dist", "tempo_traces_attrs_gin_dist",
				"samples_logs_dist", "samples_metrics_dist",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tables, distTables := healthCheckTables(tc.splitBySignal)
			if !slices.Equal(tables, tc.wantTables) {
				t.Errorf("tablesToCheck = %v, want %v", tables, tc.wantTables)
			}
			if !slices.Equal(distTables, tc.wantDistTables) {
				t.Errorf("distTablesToCheck = %v, want %v", distTables, tc.wantDistTables)
			}
		})
	}
}
