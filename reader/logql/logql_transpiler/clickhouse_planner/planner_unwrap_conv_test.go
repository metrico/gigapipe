package clickhouse_planner

import (
	"strings"
	"testing"
)

func TestUnwrapConvSQL(t *testing.T) {
	tests := []struct {
		convFunc    string
		wantContain []string
	}{
		// No conversion: plain toFloat64OrNull
		{"", []string{"toFloat64OrNull"}},
		// Duration: arraySum, NULL wrapping, sign handling, µs support
		{"duration", []string{"arraySum", "NULL", "startsWith", "µs"}},
		{"duration_seconds", []string{"extractAllGroupsHorizontal", "NULL"}},
		// Bytes: exact unit matching, NULL for unknown, comma stripping
		{"bytes", []string{"NULL", "replaceAll", "'kb'", "'eib'", "'k'"}},
	}
	for _, tc := range tests {
		got := unwrapConvSQL("labels['latency']", tc.convFunc)
		for _, want := range tc.wantContain {
			if !strings.Contains(got, want) {
				t.Errorf("unwrapConvSQL(_, %q): missing %q in:\n%s", tc.convFunc, want, got)
			}
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

// Duration and bytes SQL must contain NULL so the WHERE IS NOT NULL filter
// can drop entries that don't parse. This is critical: without it, garbage
// values silently become 0, skewing avg/min/count aggregations.
func TestUnwrapConvSQLReturnsNULL(t *testing.T) {
	for _, conv := range []string{"duration", "duration_seconds", "bytes"} {
		got := unwrapConvSQL("labels['x']", conv)
		if !strings.Contains(got, "NULL") {
			t.Errorf("unwrapConvSQL(_, %q) must return NULL for bad input, got:\n%s", conv, got)
		}
	}
}
