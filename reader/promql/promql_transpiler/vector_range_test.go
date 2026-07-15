package promql_transpiler

import (
	"strings"
	"testing"
	"time"

	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/qryn/v4/reader/config"
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v4/reader/promql/promql_parser"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

func rangeTestCtx() *shared.PlannerContext {
	if config.Cloki == nil {
		config.Cloki = clconfig.New(clconfig.CLOKI_READER, nil, "", "")
	}
	now := time.Unix(1700000000, 0)
	return &shared.PlannerContext{
		From:                    now.Add(-time.Hour),
		To:                      now,
		Step:                    time.Minute,
		TimeSeriesGinTableName:  "time_series_gin",
		SamplesTableName:        "samples_v3",
		SamplesDistTableName:    "samples_v3",
		TimeSeriesTableName:     "time_series",
		TimeSeriesDistTableName: "time_series",
		Metrics15sTableName:     "metrics_15s",
		Metrics15sDistTableName: "metrics_15s",
		Type:                    2,
	}
}

// transpileRange renders the accelerated SQL for a single range call.
func transpileRange(t *testing.T, query string) string {
	t.Helper()
	expr, err := promql_parser.Parse(query)
	if err != nil {
		t.Fatal(err)
	}
	expr, err = TranspileExpressionV2(expr)
	if err != nil {
		t.Fatal(err)
	}
	if len(expr.Substitutes) != 1 {
		t.Fatalf("%s: expected 1 substitute, got %d (not accelerated)", query, len(expr.Substitutes))
	}
	for _, s := range expr.Substitutes {
		req, err := s.Request.Process(rangeTestCtx())
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		str, err := req.String(sql.DefaultCtx())
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return str
	}
	return ""
}

// TestRangeFnsAccelerate renders every accelerated range function and asserts
// the value expression each one is expected to reduce to.
func TestRangeFnsAccelerate(t *testing.T) {
	for _, c := range []struct {
		fn   string
		want []string
	}{
		{"sum_over_time", []string{"sum(sum)", "sumIf(b_sum, source = 1)"}},
		{"count_over_time", []string{"toFloat64(countMerge(count))", "sumIf(b_cnt, source = 1)"}},
		{"min_over_time", []string{"min(min)", "minIf(b_min, source = 1)"}},
		{"max_over_time", []string{"max(max)", "maxIf(b_max, source = 1)"}},
		{"avg_over_time", []string{"sum(sum)", "toFloat64(countMerge(count))", "w_sum / w_cnt"}},
		{"last_over_time", []string{"argMaxMerge(last)", "argMaxIf(b_last, b_ts, source = 1)"}},
		{"present_over_time", []string{"1 as val", "(w_src) > (0)"}},

		{"rate", []string{"resets", "/ 300.000000", "if(open_cnt > 0, start_open, start_close)"}},
		{"increase", []string{"end - start + resets"}},
		{"delta", []string{"end - start"}},

		{"resets", []string{"(prev_cnt > 0) * (prev > val) * (source = 1)", "flags - first_flag"}},
		{"changes", []string{"(prev_cnt > 0) * (prev != val) * (source = 1)", "flags - first_flag"}},
	} {
		t.Run(c.fn, func(t *testing.T) {
			got := transpileRange(t, c.fn+`(http_requests_total{job="myjob"}[5m])`)
			for _, w := range c.want {
				if !strings.Contains(got, w) {
					t.Errorf("missing %q in:\n%s", w, got)
				}
			}
			t.Log(got)
		})
	}
}

// TestDeltaHasNoResetCorrection guards the gauge semantics of delta: a decrease
// is a real decrease, not a counter wrapping around.
func TestDeltaHasNoResetCorrection(t *testing.T) {
	got := transpileRange(t, `delta(some_gauge{job="myjob"}[5m])`)
	if strings.Contains(got, "prev > val") {
		t.Errorf("delta must not apply counter reset correction:\n%s", got)
	}
}

// TestRangeTooLargeRejected guards the int32 millisecond limit on RANGE frame
// offsets: a range beyond it must error rather than wrap into a bogus frame.
func TestRangeTooLargeRejected(t *testing.T) {
	expr, err := promql_parser.Parse(`sum_over_time(x{job="j"}[30d])`)
	if err != nil {
		t.Fatal(err)
	}
	expr, err = TranspileExpressionV2(expr)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range expr.Substitutes {
		if _, err := s.Request.Process(rangeTestCtx()); err == nil {
			t.Fatal("expected an error for a 30d range, got none")
		}
	}
}
