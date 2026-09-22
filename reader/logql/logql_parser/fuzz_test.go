package logql_parser

import "testing"

// FuzzParse asserts that Parse never panics on arbitrary input. It makes no
// claim about output shape or correctness.
//
// Not part of the default test/CI lane: native Go fuzz targets only run
// their seed corpus under plain `go test`, and are otherwise driven locally
// via `make fuzz HEAD=logql DURATION=<duration>` (or `go test -fuzz=FuzzParse`).
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

		// The rest of this seed corpus is drawn from grafana/loki's
		// pkg/logql/bench query registry — a broader spread of LogQL grammar
		// (drop/unwrap-conversion-function/detected_level, empty by()/
		// without(), chained label filters) than the hand-written seeds
		// above cover alone. Several exercise constructs this parser
		// currently rejects (see the invalid_* parser pins) — useful fuzz
		// input either way, since FuzzParse only asserts no panic.
		`{app="x"} | detected_level="error"`,
		`{app="x"} | logfmt | detected_level="error"`,
		`{app="x"} | detected_level="error" | logfmt`,
		`{app="x"} | json | detected_level="error"`,
		`{app="x"} | detected_level="error" | json`,
		`{app="x"} |= "level"`,
		`{app="x"} |~ "(?i)error"`,
		`{app="x"} !~ "(?i)debug"`,
		`{app="x"} | field = "value"`,
		`{app="x"} |= "this will not hit any line"`,
		`sum(count_over_time({app="x"}[5m]))`,
		`sum(count_over_time({app="x"} | detected_level=~"error|warn" [5m]))`,
		`sum(rate({app="x"} | detected_level=~"error|warn" [5m]))`,
		`sum(count_over_time({app="x"} |= "level" [5m]))`,
		`sum(avg_over_time({app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m]))`,
		`sum(avg_over_time({app="x"} |= "level" | logfmt | duration != "" | unwrap duration(duration) [5m]))`,
		`sum(min_over_time({app="x"} |= "level" | logfmt | duration != "" | unwrap duration(duration) [5m]))`,
		`sum(max_over_time({app="x"} |= "level" | logfmt | duration != "" | unwrap duration(duration) [5m]))`,
		`sum by (pod) (count_over_time({app="x"}[5m]))`,
		`sum by (namespace) (count_over_time({app="x"}[5m]))`,
		`sum by (service_name) (count_over_time({app="x"}[5m]))`,
		`sum by (detected_level) (count_over_time({app="x"}[5m]))`,
		`sum by (cluster, namespace) (count_over_time({app="x"}[5m]))`,
		`sum by (service_name, container) (count_over_time({app="x"}[5m]))`,
		`sum by (level) (sum_over_time({app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m]))`,
		`sum by (level) (sum_over_time({app="x"} | logfmt | duration != "" | unwrap duration_seconds(duration) [5m]))`,
		`max by (level) (min_over_time({app="x"} | logfmt | duration != "" | unwrap duration_seconds(duration) [5m]))`,
		`avg by (level) (avg_over_time({app="x"} | logfmt | duration != "" | unwrap duration_seconds(duration) [5m]))`,
		`max by () (avg_over_time({app="x"} | logfmt | duration != "" | unwrap duration_seconds(duration) [5m]))`,
		`max by (level) (avg_over_time({app="x"} | logfmt | duration != "" | unwrap duration_seconds(duration) [5m]) without (service_name))`,
		`max without () (sum_over_time({app="x"} | logfmt | duration != "" | unwrap duration_seconds(duration) [5m]))`,
		`avg_over_time({app="x"} | logfmt | duration != "" | unwrap duration_seconds(duration) [5m])`,
		`sum_over_time({app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m])`,
		`rate({app="x"} | json | unwrap duration_ms [5m])`,
		`sum by (level) (rate({app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m]))`,
		`rate({app="x"} | logfmt | duration != "" | unwrap duration_seconds(duration) [5m])`,
		`sum_over_time({app="x"} | json | unwrap duration_ms [5m])`,
		`sum(sum_over_time({app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m]))`,
		`sum by (level, namespace) (sum_over_time({app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m]))`,
		`sum without (namespace) (sum_over_time({app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m]))`,
		`sum by (level) (sum_over_time({app="x"} | logfmt | unwrap bytes [5m]))`,
		`avg_over_time({app="x"} | json | unwrap duration_ms [5m])`,
		`avg(avg_over_time({app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m]))`,
		`avg by (level) (avg_over_time({app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m]))`,
		`max_over_time({app="x"} | json | unwrap duration_ms [5m])`,
		`max(max_over_time({app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m]))`,
		`max by (level) (max_over_time({app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m]))`,
		`max by (level) (max_over_time({app="x"} | logfmt | duration != "" | unwrap duration_seconds(duration) [5m]))`,
		`min_over_time({app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m])`,
		`min(min_over_time({app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m]))`,
		`min by (level) (min_over_time({app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m]))`,
		`min by (level) (min_over_time({app="x"} | logfmt | duration != "" | unwrap duration_seconds(duration) [5m]))`,
		`sum by (detected_level) (sum_over_time({app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m]))`,
		`stdvar_over_time({app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m])`,
		`avg by (level) (stdvar_over_time({app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m]))`,
		`stddev_over_time({app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m])`,
		`avg by (level) (stddev_over_time({app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m]))`,
		`quantile_over_time(0.5, {app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m])`,
		`quantile_over_time(0.90, {app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m])`,
		`quantile_over_time(0.95, {app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m])`,
		`quantile_over_time(0.99, {app="x"} | json | unwrap duration_ms [5m])`,
		`avg by (level) (quantile_over_time(0.99, {app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m]))`,
		`quantile_over_time(0.99, {app="x"} | logfmt | duration != "" | unwrap duration_seconds(duration) [5m])`,
		`sum_over_time({app="x"} | logfmt | size != "" | unwrap size [5m])`,
		`max by () (max_over_time({app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m]))`,
		`sum without (namespace, cluster) (sum_over_time({app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m]))`,
		`max(avg by (level) (avg_over_time({app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m])))`,
		`sum(sum_over_time({app="x"} |= "level" | logfmt | duration != "" | unwrap duration(duration) [5m]))`,
		`avg(avg_over_time({app="x"} |~ "(?i)error|warn" | logfmt | duration != "" | unwrap duration(duration) [5m]))`,
		`sum by (level) (sum_over_time({app="x"} | logfmt | level != "" | duration != "" | unwrap duration(duration) [5m]))`,
		`{app="x"} |~ "error|exception" | detected_level="error"`,
		`{app="x"} | json | duration_seconds > 0.1 | detected_level!="debug"`,
		`{app="x"} | logfmt | level="error" | detected_level="error"`,
		`{app="x"} |~ "(?i)error" | json | logfmt | drop __error__, __error_details__ | status_code >= 500`,
		`{app="x"} | detected_level="error" |~ "(?i)failed"`,
		`{app="x"} | detected_level=~"error|warn" |~ "(?i)refused"`,
		`{app="x"} | json | logfmt | drop __error__, __error_details__`,
		`{app="x"} | json | logfmt | drop __error__, __error_details__ | level="error"`,
		`{app="x"} | json | logfmt | drop __error__, __error_details__ | level="error" or level="warn"`,
		`sum by (detected_level) (avg_over_time({app="x"} | json | logfmt | duration != "" | drop __error__, __error_details__ | unwrap duration_seconds(duration) [5m]))`,
		`sum by (detected_level) (count_over_time({app="x"} | detected_level="debug" or detected_level="info" or detected_level="warn" |~ "(?i)(?i)duration" | json | logfmt | drop __error__, __error_details__ | level=~"(?i)INFO" [5m]))`,
		`rate({app="x"} | logfmt | duration != "" | unwrap duration(duration) [5m])`,
		`sum by (level) (avg_over_time({app="x"} | logfmt | duration != "" | unwrap duration_seconds(duration) [5m]))`,
		`sum(sum_over_time({app="x"} | logfmt | duration != "" | unwrap duration_seconds(duration) [5m]))`,
		`avg(avg_over_time({app="x"} | logfmt | duration != "" | unwrap duration_seconds(duration) [5m]))`,
		`sum(sum_over_time({app="x"} | logfmt | unwrap bytes [5m]))`,
		`sum(sum_over_time({app="x"} | logfmt | size != "" | unwrap size [5m]))`,
		`avg(avg_over_time({app="x"} | logfmt | size != "" | unwrap size [5m]))`,
		`avg(avg_over_time({app="x"} | logfmt | unwrap status [5m]))`,
		`sum by (level) (count_over_time({app="x"} | json [5m]))`,
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
