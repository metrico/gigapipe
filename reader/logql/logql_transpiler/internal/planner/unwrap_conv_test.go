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

		// duration / duration_seconds — happy path
		{"5m30s", "duration", 330, false},
		{"1h", "duration", 3600, false},
		{"500ms", "duration", 0.5, false},
		{"2s", "duration_seconds", 2, false},
		{"1m30s", "duration_seconds", 90, false},

		// duration — edge cases
		{"-5s", "duration", -5, false},
		{"-1h30m", "duration", -5400, false},
		{"300µs", "duration", 0.0003, false},
		{"300us", "duration", 0.0003, false},
		{"bad", "duration", 0, true},
		{"5", "duration", 0, true},   // no unit
		{"1d", "duration", 0, true},  // invalid unit
		{"abc", "duration", 0, true}, // garbage
		{"", "duration", 0, true},    // empty

		// bytes — happy path
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
		{"1eb", "bytes", 1000000000000000000, false},
		{"1eib", "bytes", 1152921504606846976, false},

		// bytes — short forms
		{"1k", "bytes", 1000, false},
		{"1K", "bytes", 1000, false},
		{"1m", "bytes", 1000000, false},
		{"1g", "bytes", 1000000000, false},
		{"1t", "bytes", 1000000000000, false},
		{"1p", "bytes", 1000000000000000, false},
		{"1e", "bytes", 1000000000000000000, false},
		{"1b", "bytes", 1, false},
		{"1ki", "bytes", 1024, false},
		{"1mi", "bytes", 1048576, false},

		// bytes — commas
		{"1,024 KB", "bytes", 1024000, false},
		{"1,000,000", "bytes", 1000000, false},

		// bytes — must error
		{"", "bytes", 0, true},
		{"abc", "bytes", 0, true},
		{"kb", "bytes", 0, true},    // no number
		{"-1kb", "bytes", 0, true},  // negative (no leading digit)
		{"1xyz", "bytes", 0, true},  // unknown unit
		{"1 foo", "bytes", 0, true}, // unknown unit
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
