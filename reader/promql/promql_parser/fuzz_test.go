package promql_parser

import "testing"

// FuzzParse asserts that Parse never panics on arbitrary input. It makes no
// claim about output shape or correctness — see the golden corpus for that.
//
// Not part of the default test/CI lane: native Go fuzz targets only run
// their seed corpus under plain `go test`, and are otherwise driven locally
// via `just fuzz promql [duration]` (or `go test -fuzz=FuzzParse`).
func FuzzParse(f *testing.F) {
	for _, seed := range []string{
		`http_requests_total`,
		`http_requests_total{status="5xx"}`,
		`rate(http_requests_total{status="5xx"}[5m])`,
		`increase(http_requests_total[1h])`,
		`sum(rate(http_requests_total[5m])) by (status)`,
		`sum without (instance) (rate(http_requests_total[5m]))`,
		`avg(node_cpu_seconds_total) by (cpu) > 0.5`,
		`topk(5, sum(rate(http_requests_total[5m])) by (path))`,
		`histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))`,
		`(node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes) / node_memory_MemTotal_bytes`,
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
