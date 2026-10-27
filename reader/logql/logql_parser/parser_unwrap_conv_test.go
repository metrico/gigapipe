package logql_parser

import (
	"testing"
)

func TestParserUnwrapConvFunctions(t *testing.T) {
	tests := []struct {
		query    string
		convFunc string
		label    string
	}{
		{
			`sum_over_time({app="x"} | json | unwrap duration(latency) [5m])`,
			"duration", "latency",
		},
		{
			`sum_over_time({app="x"} | json | unwrap duration_seconds(latency) [5m])`,
			"duration_seconds", "latency",
		},
		{
			`sum_over_time({app="x"} | json | unwrap bytes(size) [5m])`,
			"bytes", "size",
		},
	}
	for _, tc := range tests {
		ast, err := Parse(tc.query)
		if err != nil {
			t.Fatalf("Parse(%q): %v", tc.query, err)
		}
		unwrap := FindFirst[Unwrap](ast)
		if unwrap == nil {
			t.Fatalf("Parse(%q): no Unwrap node in AST", tc.query)
		}
		if unwrap.ConvFunc() != tc.convFunc {
			t.Errorf("Parse(%q): ConvFunc() = %q, want %q", tc.query, unwrap.ConvFunc(), tc.convFunc)
		}
		if unwrap.LabelName() != tc.label {
			t.Errorf("Parse(%q): LabelName() = %q, want %q", tc.query, unwrap.LabelName(), tc.label)
		}
	}
}

// A bare `unwrap duration` (no parens) must still parse as a plain label
// named "duration", preserving backward compatibility.
func TestParserUnwrapBareKeywordLabels(t *testing.T) {
	tests := []struct {
		query string
		label string
	}{
		{`sum_over_time({app="x"} | json | unwrap duration [5m])`, "duration"},
		{`sum_over_time({app="x"} | json | unwrap bytes [5m])`, "bytes"},
		{`sum_over_time({app="x"} | json | unwrap duration_seconds [5m])`, "duration_seconds"},
	}
	for _, tc := range tests {
		ast, err := Parse(tc.query)
		if err != nil {
			t.Fatalf("Parse(%q): %v", tc.query, err)
		}
		unwrap := FindFirst[Unwrap](ast)
		if unwrap == nil {
			t.Fatalf("Parse(%q): no Unwrap node in AST", tc.query)
		}
		if unwrap.ConvFunc() != "" {
			t.Errorf("Parse(%q): ConvFunc() = %q, want empty", tc.query, unwrap.ConvFunc())
		}
		if unwrap.LabelName() != tc.label {
			t.Errorf("Parse(%q): LabelName() = %q, want %q", tc.query, unwrap.LabelName(), tc.label)
		}
	}
}
