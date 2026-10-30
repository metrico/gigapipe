package logql_parser

import "testing"

// TestLabelFilterStringDanglingOp checks that rendering does not panic when the
// parser accepts "and"/"or" with nothing after it. Such input is not valid
// LogQL; this only guards String(), it does not decide whether the parser
// should accept it. The last case is the input the parser fuzz target found.
func TestLabelFilterStringDanglingOp(t *testing.T) {
	for _, query := range []string{
		`{app="x"} | a="b" or`,
		`{app="x"} | a="b" and`,
		`{app="x"} | json | a="b" or`,
		`{A=""}|json|logfmt|drop A000|A=""or`,
	} {
		t.Run(query, func(t *testing.T) {
			rendered := renderQuery(t, query)
			if again := renderQuery(t, rendered); again != rendered {
				t.Fatalf("%q renders as %q, which renders as %q", query, rendered, again)
			}
		})
	}
}

// TestLabelFilterStringBinary checks that a complete and/or label filter still
// renders both sides.
func TestLabelFilterStringBinary(t *testing.T) {
	cases := []struct {
		query string
		want  string
	}{
		{`{app="x"} | a="b" or c="d"`, `{app="x"}| a = "b" or c = "d"`},
		{`{app="x"} | a="b" and c="d"`, `{app="x"}| a = "b" and c = "d"`},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			if got := renderQuery(t, tc.query); got != tc.want {
				t.Fatalf("String() = %q, want %q", got, tc.want)
			}
		})
	}
}

func renderQuery(t *testing.T, query string) (out string) {
	t.Helper()
	script, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse(%q) error = %v", query, err)
	}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("String() panicked on %q: %v", query, r)
		}
	}()
	return script.String()
}
