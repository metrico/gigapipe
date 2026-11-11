package prof_parser

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// FuzzParse asserts that Parse never panics on arbitrary input, that any
// selector it accepts round-trips (rendering it, parsing the rendering and
// rendering that again gives the same text), and that unquoting an accepted
// value, the first thing the query planner does with it, never panics. It
// makes no claim about which queries should be accepted.
//
// The seed corpus below (and any inputs under testdata/fuzz/FuzzParse) runs
// as an ordinary test under `go test ./...`, so CI checks it on every change.
// Fuzzing beyond the seeds is not part of CI: it runs with
// `make fuzz HEAD=prof DURATION=<duration>`, locally or from the manually
// dispatched "Parser fuzz" workflow.
func FuzzParse(f *testing.F) {
	for _, seed := range []string{
		`{}`,
		`{service_name="checkout"}`,
		`{__name__="process_cpu"}`,
		`{service_name="checkout", __name__="process_cpu", __period_type__="cpu"}`,
		`{service_name!="checkout"}`,
		`{service_name=~"api-.*"}`,
		`{service_name!~"api-.*"}`,
		"{service_name=`checkout`}",
		`{service_name="checkout",}`,
		`{ service_name = "checkout" , env = "prod" }`,
		`{"service.version"="1.4.0"}`,
		`{"service_name"="a"}`,
		`{"a\"b"="x"}`,
		`{"k8s.pod.name"!~"api-.*"}`,
		`{service_name="checkout", "service.version"="1.4.0"}`,
		`{service_name="a\nb"}`,
		`{service.version="1.4.0"}`,
		`{service_name=}`,
		`{,}`,
		`service_name="checkout"`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, query string) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panicked on %q: %v", query, r)
			}
		}()
		script, err := Parse(query)
		if err != nil {
			return
		}
		for _, s := range script.Selectors {
			_, _ = s.Val.Unquote()
		}
		rendered := render(script)
		again, err := Parse(rendered)
		if err != nil {
			t.Fatalf("%q renders as %q, which does not parse: %v", query, rendered, err)
		}
		if got := render(again); got != rendered {
			t.Fatalf("%q renders as %q, which renders as %q", query, rendered, got)
		}
	})
}

var bareLabelName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// render writes a parsed selector back as text. The parser has no String
// method, so the fuzz target renders here: a name that is not a bare
// identifier is written in double quotes, values keep the quoting they were
// parsed with.
func render(script *Script) string {
	parts := make([]string, len(script.Selectors))
	for i, s := range script.Selectors {
		name := string(s.Name)
		if !bareLabelName.MatchString(name) {
			name = strconv.Quote(name)
		}
		parts[i] = name + s.Op + s.Val.Str
	}
	return "{" + strings.Join(parts, ",") + "}"
}
