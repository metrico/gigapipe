package clickhouse_planner

import (
	"strings"
	"testing"
)

func TestUnwrapConvSQL(t *testing.T) {
	tests := []struct {
		convFunc    string
		wantContain string
	}{
		// No conversion: plain toFloat64OrNull
		{"", "toFloat64OrNull"},
		// Duration: uses extractAllGroupsHorizontal + arraySum
		{"duration", "arraySum"},
		{"duration_seconds", "extractAllGroupsHorizontal"},
		// Bytes: uses extract + LIKE checks
		{"bytes", "LIKE '%kb%'"},
	}
	for _, tc := range tests {
		got := unwrapConvSQL("labels['latency']", tc.convFunc)
		if !strings.Contains(got, tc.wantContain) {
			t.Errorf("unwrapConvSQL(_, %q) = %q, want substring %q", tc.convFunc, got, tc.wantContain)
		}
	}
}

// Default (empty convFunc) must produce the same output as the old code.
func TestUnwrapConvSQLDefault(t *testing.T) {
	got := unwrapConvSQL("labels['x']", "")
	want := "toFloat64OrNull(labels['x'])"
	if got != want {
		t.Errorf("unwrapConvSQL default = %q, want %q", got, want)
	}
}
