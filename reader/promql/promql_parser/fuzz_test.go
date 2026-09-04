package promql_parser

import "testing"

// FuzzParse asserts that Parse never panics on arbitrary input. It makes no
// claim about output shape or correctness.
//
// Not part of the default test/CI lane: native Go fuzz targets only run
// their seed corpus under plain `go test`, and are otherwise driven locally
// via `make fuzz HEAD=promql DURATION=<duration>` (or `go test -fuzz=FuzzParse`).
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

		// The rest of this seed corpus is drawn from the prometheus/compliance
		// project's query set (github.com/prometheus/compliance, promql
		// subdirectory) — a broader spread of PromQL grammar (vector-matching
		// modifiers, date/math functions, label_replace/label_join, special
		// values, subqueries) than the hand-written seeds above cover alone.
		`42`,
		`1.234`,
		`.123`,
		`1.23e-3`,
		`0x3d`,
		`Inf`,
		`+Inf`,
		`-Inf`,
		`NaN`,
		`demo_memory_usage_bytes`,
		`{__name__="demo_memory_usage_bytes"}`,
		`demo_memory_usage_bytes{type="free"}`,
		`demo_memory_usage_bytes{type!="free"}`,
		`demo_memory_usage_bytes{instance=~"demo.promlabs.com:.*"}`,
		`demo_memory_usage_bytes{instance=~"host"}`,
		`demo_memory_usage_bytes{instance!~".*:10000"}`,
		`demo_memory_usage_bytes{type="free", instance!="demo.promlabs.com:10000"}`,
		`{type="free", instance!="demo.promlabs.com:10000"}`,
		`{__name__=~".*"}`,
		`nonexistent_metric_name`,
		`demo_memory_usage_bytes offset 1m`,
		`demo_memory_usage_bytes offset -5m`,
		`demo_intermittent_metric`,
		`sum(demo_memory_usage_bytes)`,
		`avg(nonexistent_metric_name)`,
		`max by() (demo_memory_usage_bytes)`,
		`min by(instance) (demo_memory_usage_bytes)`,
		`count by(instance, type) (demo_memory_usage_bytes)`,
		`stddev by(nonexistent) (demo_memory_usage_bytes)`,
		`stdvar without() (demo_memory_usage_bytes)`,
		`sum without(instance) (demo_memory_usage_bytes)`,
		`avg without(instance, type) (demo_memory_usage_bytes)`,
		`max without(nonexistent) (demo_memory_usage_bytes)`,
		`topk (3, demo_memory_usage_bytes)`,
		`bottomk by(instance) (2, demo_memory_usage_bytes)`,
		`topk without(instance) (2, demo_memory_usage_bytes)`,
		`bottomk without() (2, demo_memory_usage_bytes)`,
		`quantile(-0.5, demo_memory_usage_bytes)`,
		`avg(max by(type) (demo_memory_usage_bytes))`,
		`1 * 2 + 4 / 6 - 10 % 2 ^ 2`,
		`demo_num_cpus + (1 == bool 2)`,
		`demo_memory_usage_bytes + 1.2345`,
		`demo_memory_usage_bytes != bool 1.2345`,
		`1.2345 < bool demo_memory_usage_bytes`,
		`0.12345 - demo_memory_usage_bytes`,
		`(1 * 2 + 4 / 6 - (10%7)^2) * demo_memory_usage_bytes`,
		`demo_memory_usage_bytes / (1 * 2 + 4 / 6 - 10)`,
		`timestamp(demo_memory_usage_bytes * 1)`,
		`timestamp(-demo_memory_usage_bytes)`,
		`demo_memory_usage_bytes % on(instance, job, type) demo_memory_usage_bytes`,
		`sum by(instance, type) (demo_memory_usage_bytes) ^ on(instance, type) group_left(job) demo_memory_usage_bytes`,
		`demo_memory_usage_bytes > bool on(instance, job, type) demo_memory_usage_bytes`,
		`demo_memory_usage_bytes / on(instance, job, type, __name__) demo_memory_usage_bytes`,
		`sum without(job) (demo_memory_usage_bytes) / on(instance, type) demo_memory_usage_bytes`,
		`sum without(job) (demo_memory_usage_bytes) / on(instance, type) group_left demo_memory_usage_bytes`,
		`sum without(job) (demo_memory_usage_bytes) / on(instance, type) group_left(job) demo_memory_usage_bytes`,
		`demo_memory_usage_bytes / on(instance, job) group_left demo_num_cpus`,
		`demo_memory_usage_bytes / on(instance, type, job, non_existent) demo_memory_usage_bytes`,
		`demo_num_cpus * Inf`,
		`demo_num_cpus * -Inf`,
		`demo_num_cpus * NaN`,
		`demo_memory_usage_bytes + -(1)`,
		`-demo_memory_usage_bytes`,
		`-1 ^ 2`,
		`1 + time()`,
		`time() - 1`,
		`time() <= bool 1`,
		`1 >= bool time()`,
		`time() * time()`,
		`time() == bool time()`,
		`time() == demo_memory_usage_bytes`,
		`demo_memory_usage_bytes != time()`,
		`sum_over_time(demo_memory_usage_bytes[1s])`,
		`quantile_over_time(0.1, demo_memory_usage_bytes[15s])`,
		`timestamp(demo_num_cpus)`,
		`timestamp(timestamp(demo_num_cpus))`,
		`abs(demo_memory_usage_bytes)`,
		`ceil(-demo_memory_usage_bytes)`,
		`delta(nonexistent_metric[5m])`,
		`rate(demo_cpu_usage_seconds_total[1m])`,
		`deriv(demo_disk_usage_bytes[5m])`,
		`predict_linear(demo_disk_usage_bytes[15m], 600)`,
		`time()`,
		`label_replace(demo_num_cpus, "job", "destination-value-$1", "instance", "demo.promlabs.com:(.*)")`,
		`label_replace(demo_num_cpus, "job", "destination-value-$1", "instance", "host:(.*)")`,
		`label_replace(demo_num_cpus, "job", "$1-$2", "instance", "local(.*):(.*)")`,
		`label_replace(demo_num_cpus, "job", "value-$1", "nonexistent-src", "source-value-(.*)")`,
		`label_replace(demo_num_cpus, "job", "value-$1", "nonexistent-src", "(.*)")`,
		`label_replace(demo_num_cpus, "job", "value-$1", "instance", "non-matching-regex")`,
		`label_replace(demo_num_cpus, "job", "", "dst", ".*")`,
		`label_replace(demo_num_cpus, "job", "value-$1", "src", "(.*")`,
		`label_replace(demo_num_cpus, "~invalid", "", "src", "(.*)")`,
		`label_replace(demo_num_cpus, "instance", "", "", "")`,
		`label_join(demo_num_cpus, "new_label", "-", "instance", "job")`,
		`label_join(demo_num_cpus, "job", "-", "instance", "job")`,
		`label_join(demo_num_cpus, "job", "-", "instance")`,
		`label_join(demo_num_cpus, "~invalid", "-", "instance")`,
		`day_of_month()`,
		`day_of_week(demo_batch_last_success_timestamp_seconds offset 10m)`,
		`idelta(demo_cpu_usage_seconds_total[1h])`,
		`clamp_min(demo_memory_usage_bytes, 2)`,
		`clamp(demo_memory_usage_bytes, 0, 1)`,
		`clamp(demo_memory_usage_bytes, 0, 1000000000000)`,
		`clamp(demo_memory_usage_bytes, 1000000000000, 0)`,
		`clamp(demo_memory_usage_bytes, 1000000000000, 1000000000000)`,
		`resets(demo_cpu_usage_seconds_total[1s])`,
		`changes(demo_batch_last_success_timestamp_seconds[15s])`,
		`vector(1.23)`,
		`vector(time())`,
		`histogram_quantile(0.5, rate(demo_api_request_duration_seconds_bucket[1m]))`,
		`histogram_quantile(0.9, nonexistent_metric)`,
		`histogram_quantile(0.9, demo_memory_usage_bytes)`,
		`histogram_quantile(0.9, {__name__=~"demo_api_request_duration_seconds_.+"})`,
		`count_values("value", demo_api_request_duration_seconds_bucket)`,
		`absent(demo_memory_usage_bytes)`,
		`absent(nonexistent_metric_name)`,
		`max_over_time((time() - max(demo_batch_last_success_timestamp_seconds) < 1000)[5m:10s] offset 5m)`,
		`avg_over_time(rate(demo_cpu_usage_seconds_total[1m])[2m:10s])`,
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
