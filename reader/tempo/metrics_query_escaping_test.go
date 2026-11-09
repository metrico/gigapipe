package tempo

import (
	"strings"
	"testing"
)

// Regex filter values and by() labels reach ClickHouse inside string literals.
// A backslash in either must not change where the literal ends, or the value
// becomes part of the query text.

func renderRegexFilter(t *testing.T, op, val string) string {
	t.Helper()
	traces, attrs := metricsTables(false)
	q, _, err := BuildMetricsRangeQuery("rate", "", nil,
		[]string{"span.foo"}, []string{op}, []string{val},
		fromNS, toNS, stepNS, traces, attrs, false, nil)
	return render(t, q, err)
}

func TestRegexFilterValueIsEscaped(t *testing.T) {
	for _, tc := range []struct{ name, val, want string }{
		{"trailing backslash", `\`, `match(val, '\\')`},
		{"backslash before a quote", `\') OR 1=1 -- `, `match(val, '\\\') OR 1=1 -- ')`},
		{"quote", `it's`, `match(val, 'it\'s')`},
		{"regex class", `^a\d+$`, `match(val, '^a\\d+$')`},
	} {
		for _, op := range []string{"=~", "!~"} {
			t.Run(tc.name+" "+op, func(t *testing.T) {
				if got := renderRegexFilter(t, op, tc.val); !strings.Contains(got, tc.want) {
					t.Fatalf("want %s in:\n%s", tc.want, got)
				}
			})
		}
	}
}

func TestByLabelIsEscaped(t *testing.T) {
	_, attrs := metricsTables(false)
	with, _ := buildByPivotCTE([]string{`span.a\'b`}, "2023-11-14", "2023-11-14", attrs)
	got := render(t, with.GetQuery(), nil)
	if want := `anyIf(val, key = 'a\\\'b')`; !strings.Contains(got, want) {
		t.Fatalf("want %s in:\n%s", want, got)
	}
}
