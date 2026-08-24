package traceql_parser

import "testing"

// FuzzParse asserts that Parse never panics on arbitrary input. It makes no
// claim about output shape or correctness — see the golden corpus for that.
//
// Not part of the default test/CI lane: native Go fuzz targets only run
// their seed corpus under plain `go test`, and are otherwise driven locally
// via `make fuzz HEAD=traceql DURATION=<duration>` (or `go test -fuzz=FuzzParse`).
func FuzzParse(f *testing.F) {
	for _, seed := range []string{
		`{.service.name="test"}`,
		`{.randomContainer=~"admiring" && .randomFloat > 10}`,
		`{.randomContainer=~"admiring" && .randomFloat > 10} | count() > 2 || {.randomContainer=~"boring" && .randomFloat < 10}`,
		`{duration > 100ms}`,
		`{name="GET /api"}`,
		`{status = error}`,
		`{span.http.method="GET" && resource.service.name="frontend"}`,
		`{} >> {name="child"}`,
		`{.foo="bar"} | avg(duration) > 100ms`,
		`{.foo=~"bar.*"}`,
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, query string) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Parse panicked on %q: %v", query, r)
			}
		}()
		_, _ = Parse(query)
	})
}
