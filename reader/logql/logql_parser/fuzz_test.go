package logql_parser

import "testing"

// FuzzParse asserts that Parse never panics on arbitrary input. It makes no
// claim about output shape or correctness — see the golden corpus for that.
//
// Not part of the default test/CI lane: native Go fuzz targets only run
// their seed corpus under plain `go test`, and are otherwise driven locally
// via `just fuzz logql [duration]` (or `go test -fuzz=FuzzParse`).
func FuzzParse(f *testing.F) {
	for _, seed := range []string{
		`{app="x"}`,
		`{app="x", env="prod"}`,
		`{app="x"} |= "GET"`,
		`{app="x"} |~ "2[0-9]$"`,
		`{app="x"} != "error" != "debug"`,
		`{app="x"} | json`,
		`{app="x"} | json field="value"`,
		`{app="x"} | logfmt`,
		`{app="x"} | regexp "^(?<e>[^0-9]+)[0-9]+$"`,
		`rate({app="x"} |= "GET" [5m])`,
		`sum by (app) (rate({app="x"} [1m]))`,
		`sum_over_time({app="x"} | json | unwrap value [1m]) by (app)`,
		`{app="x"} | line_format "{{.field}}"`,
		`{app="x"} | freq > 4 and (status="4" or status==2)`,
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
