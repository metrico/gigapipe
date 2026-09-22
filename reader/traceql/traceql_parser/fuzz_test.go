package traceql_parser

import "testing"

// FuzzParse asserts that Parse never panics on arbitrary input. It makes no
// claim about output shape or correctness.
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

		// The rest of this seed corpus is adapted from a TraceQL compatibility
		// smoke suite (query strings only, no fixture/expectation metadata) —
		// a broader spread of TraceQL grammar (structural operators, metrics
		// pipelines with by(), nil comparisons, colon-form intrinsics,
		// attribute-to-attribute comparisons) than the hand-written seeds
		// above cover alone. Several exercise constructs this parser
		// currently rejects (see the *_not_supported parser pins) — useful
		// fuzz input either way, since FuzzParse only asserts no panic.
		`{ resource.service.name = "checkout" }`,
		`{ resource.service.name != "checkout" }`,
		`{ resource.service.name =~ "ship.*" }`,
		`{ resource.service.name =~ "check" }`,
		`{ resource.deployment.env = "compat-test" }`,
		`{ span.http.method = "GET" }`,
		`{ .name = "attr-name-collision-sentinel" }`,
		`{ duration > 100ms }`,
		`{ duration < 300ms }`,
		`{ kind = server }`,
		`{ status = error }`,
		`{ resource.service.name = "payments" } > { kind = consumer }`,
		`{ kind = consumer } < { resource.service.name = "payments" }`,
		`{ resource.service.name = "checkout" } >> { kind = client }`,
		`{ kind = client } << { resource.service.name = "checkout" }`,
		`{ resource.service.name = "checkout" && status = error }`,
		`{ resource.service.name = "checkout" } || { resource.service.name = "payments" }`,
		`{ kind = server } || { kind = internal }`,
		`{ resource.service.name =~ ".+" } | count() > 0`,
		`{ status = ok } | avg(duration) > 0`,
		`{ status = error } | count() > 0`,
		`{ resource.service.name = "checkout" && span.http.method = "GET" } && { span.child.index = "0" }`,
		`{ resource.service.name = "payments" }`,
		`{ resource.service.name = "checkout" } | select(nestedSetParent, nestedSetLeft, nestedSetRight)`,
		`({ nestedSetParent < 0 } &>> { kind = server }) || ({ nestedSetParent < 0 }) | select(status, resource.service.name, name, nestedSetParent, nestedSetLeft, nestedSetRight)`,
		`{ } | rate()`,
		`{ } | count_over_time() by (resource.service.name)`,
		`{ } | compare({ status = error }, 500)`,
		`{ } | quantile_over_time(duration, 0.95)`,
		`{ } | histogram_over_time(duration)`,
		`{ } | count_over_time()`,
		`{ } | avg_over_time(duration)`,
		`{ }`,
		`{{{`,
		`{ span.http.method = "GET" && resource.cluster = }`,
		`{ span.child.index = "0" }`,
		`{ resource.service.name = "checkout" && resource.cluster = }`,
		`{ kind != nil }`,
		`{ kind != nil && span.child.index = "0" && span.kind = "SPAN_KIND_UNSPECIFIED" }`,
		`{ status != nil }`,
		`{ name != nil }`,
		`{ span.http.method = nil }`,
		`{ resource.deployment.env != nil }`,
		`{nestedSetParent<0 && true && kind != nil} | rate() by(kind)`,
		`{ kind != nil } | rate() by(kind)`,
		`{ trace:rootService = "checkout" }`,
		`{ trace:rootName =~ "GET /api/.*" }`,
		`{ trace:duration > 100ms }`,
		`{ !(kind = client) }`,
		`{ span:kind = server }`,
		`{ span:name =~ "GET /api/.*" }`,
		`{ span:duration > 100ms }`,
		`{ span:status = error }`,
		`{ span:id != nil }`,
		`{ span:parentID != nil }`,
		`{ instrumentation:version = "1" }`,
		`{ event:name = "whatever" }`,
		`{ link:traceID != nil }`,
		`{ ("foo" != "bar") && !("foo" = "bar") }`,
		`{ span.child.index = resource.deployment.env }`,
		`{ duration > span.budget_ns }`,
		`{ name > 3 }`,
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
