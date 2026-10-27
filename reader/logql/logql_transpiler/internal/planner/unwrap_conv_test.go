package planner

import (
	"math"
	"testing"
)

func TestUnwrapConvert(t *testing.T) {
	tests := []struct {
		val      string
		convFunc string
		want     float64
		wantErr  bool
	}{
		// Plain float (no conversion function)
		{"42.5", "", 42.5, false},
		{"0", "", 0, false},
		{"not_a_number", "", 0, true},

		// duration / duration_seconds
		{"5m30s", "duration", 330, false},
		{"1h", "duration", 3600, false},
		{"500ms", "duration", 0.5, false},
		{"2s", "duration_seconds", 2, false},
		{"1m30s", "duration_seconds", 90, false},
		{"bad", "duration", 0, true},

		// bytes
		{"1024", "bytes", 1024, false},
		{"1.5kb", "bytes", 1500, false},
		{"1.5KB", "bytes", 1500, false},
		{"2kib", "bytes", 2048, false},
		{"1mb", "bytes", 1000000, false},
		{"1mib", "bytes", 1048576, false},
		{"1gb", "bytes", 1000000000, false},
		{"1gib", "bytes", 1073741824, false},
		{"1tb", "bytes", 1000000000000, false},
		{"1tib", "bytes", 1099511627776, false},
		{"1pb", "bytes", 1000000000000000, false},
		{"1pib", "bytes", 1125899906842624, false},
	}
	for _, tc := range tests {
		got, err := unwrapConvert(tc.val, tc.convFunc)
		if tc.wantErr {
			if err == nil {
				t.Errorf("unwrapConvert(%q, %q) = %v, want error", tc.val, tc.convFunc, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("unwrapConvert(%q, %q): unexpected error: %v", tc.val, tc.convFunc, err)
			continue
		}
		if math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("unwrapConvert(%q, %q) = %v, want %v", tc.val, tc.convFunc, got, tc.want)
		}
	}
}
