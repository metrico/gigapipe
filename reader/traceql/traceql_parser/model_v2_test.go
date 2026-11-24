package traceql_parser

import "testing"

// TestStringEmptySelector checks that a query with an empty selector, which is
// valid TraceQL and matches every span, renders back to TraceQL that parses to
// the same query.
func TestStringEmptySelector(t *testing.T) {
	cases := []struct {
		query string
		want  string
	}{
		{`{}`, `{}`},
		{`{ }`, `{}`},
		{`({ })`, `({})`},
		{`{ } | rate()`, `{} | rate()`},
		{`{ } | count() > 1`, `{} | count() > 1`},
		{`{ } | rate() | topk(3)`, `{} | rate() | topk(3)`},
		{`{ } && { .a = 1 }`, `{} && {.a = 1}`},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			got := renderQuery(t, tc.query)
			if got != tc.want {
				t.Fatalf("String() = %q, want %q", got, tc.want)
			}
			if again := renderQuery(t, got); again != got {
				t.Fatalf("rendering %q again gives %q", got, again)
			}
		})
	}
}

// TestStringDanglingAndOr checks that rendering does not panic when the parser
// accepts && or || with nothing after it. Such input is not valid TraceQL; this
// only guards String(), it does not decide whether the parser should accept it.
func TestStringDanglingAndOr(t *testing.T) {
	for _, query := range []string{`{ .a = 1 && }`, `{ .a = 1 || }`} {
		t.Run(query, func(t *testing.T) {
			renderQuery(t, query)
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
