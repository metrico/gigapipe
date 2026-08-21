package promql_transpiler

import (
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// rangeTestCtxCap builds a planner context. staleness selects whether the server
// is reported to support WITH FILL STALENESS (clickhouse >= 24.11), which picks
// the fill path in bucketedValues.
func rangeTestCtxCap(staleness bool) *shared.PlannerContext {
	now := time.Unix(1700000000, 0)
	var ver dbversion.VersionInfo
	if staleness {
		ver = dbversion.VersionInfo{dbversion.CapStaleness: 0}
	}
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
		VersionInfo:             ver,
	}
}

// rangeTestCtx is the default context: a modern, STALENESS-capable server.
func rangeTestCtx() *shared.PlannerContext {
	return rangeTestCtxCap(true)
}

// transpileRange renders the accelerated SQL for a single range call.
func transpileRange(t *testing.T, query string) string {
	t.Helper()
	return transpileRangeCtx(t, query, rangeTestCtx())
}

func transpileRangeCtx(t *testing.T, query string, ctx *shared.PlannerContext) string {
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
		req, err := s.Request.Process(ctx)
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

		{"rate", []string{"last_v - first_v + (resets - first_reset) as c_change", "c_change / c_span_sec as val"}},
		{"increase", []string{"last_v - first_v + (resets - first_reset) as c_change", "c_change / c_span_sec * 300.000000 as val"}},
		{"delta", []string{"last_v - first_v as c_change", "c_change / c_span_sec * 300.000000 as val"}},

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

// TestCounterNeedsTwoSamples guards the leading edge of a series. rate, increase
// and delta measure a change across an interval, so a single sample -- which
// spans no interval at all -- must yield no point rather than a zero. The same
// condition is what keeps c_span_sec, the divisor, off zero.
func TestCounterNeedsTwoSamples(t *testing.T) {
	for _, fn := range []string{"rate", "increase", "delta"} {
		t.Run(fn, func(t *testing.T) {
			got := transpileRange(t, fn+`(x{job="j"}[1m])`)
			if !strings.Contains(got, "((last_ts) > (first_ts))") {
				t.Errorf("%s must require an interval to measure across:\n%s", fn, got)
			}
		})
	}
}

// TestCounterSpanIsBetweenSamplesNotBuckets guards the pairing of the two
// quantities the slope is built from.
//
// val is the last sample of its bucket, so it sits at val_ts, which is not the
// bucket key: it lands wherever the last sample of that bucket happened to fall.
// The final bucket of a query is truncated by the read bound, so its last sample
// can be anywhere inside it. Measuring the change between samples but the
// interval between bucket keys therefore divides by an interval the change did
// not happen over, and understates every counter function at the last step.
func TestCounterSpanIsBetweenSamplesNotBuckets(t *testing.T) {
	for _, fn := range []string{"rate", "increase", "delta"} {
		t.Run(fn, func(t *testing.T) {
			got := transpileRange(t, fn+`(x{job="j"}[5m])`)
			for _, w := range []string{
				"intDiv(max(timestamp_ns), 1000000) as val_ts",
				"argMinIf(val_ts, timestamp_ms, source = 1) OVER cnt_close_wnd as first_ts",
				"argMaxIf(val_ts, timestamp_ms, source = 1) OVER cnt_close_wnd as last_ts",
				"(last_ts - first_ts) / 1000 as c_span_sec",
			} {
				if !strings.Contains(got, w) {
					t.Errorf("%s: missing %q -- the span must come from the sampled\n"+
						"timestamps, not the bucket keys:\n%s", fn, w, got)
				}
			}
			// The bucket key is timestamp_ms. Selecting it directly as an
			// endpoint is the mistake this test exists to catch.
			for _, bad := range []string{
				"minIf(timestamp_ms, source = 1) OVER cnt_close_wnd as first_ts",
				"maxIf(timestamp_ms, source = 1) OVER cnt_close_wnd as last_ts",
			} {
				if strings.Contains(got, bad) {
					t.Errorf("%s: %q measures the interval between buckets, not\n"+
						"between the two sampled values:\n%s", fn, bad, got)
				}
			}
		})
	}
}

// TestRateDividesBySpanNotRange guards the defect that motivated this shape.
// Dividing the observed change by the full range assumes the samples covered all
// of it. Whenever the newest sample lags the step -- always true at the last step
// of a window, and common mid-window with jittered scrapes -- they did not, and
// the result is understated in proportion to how much of the range they missed.
func TestRateDividesBySpanNotRange(t *testing.T) {
	got := transpileRange(t, `rate(x{job="j"}[5m])`)
	if !strings.Contains(got, "c_change / c_span_sec as val") {
		t.Errorf("rate must divide by the interval the change was measured over:\n%s", got)
	}
	if strings.Contains(got, "c_change / 300.000000") ||
		strings.Contains(got, "c_span_sec * 300.000000 as val") {
		t.Errorf("rate must not normalise by the range:\n%s", got)
	}
}

// TestIncreaseIsRateOverTheRange guards the one difference between them: increase
// reports the same slope over the whole range instead of per second, which is why
// it is a counter function that is not a rate one.
func TestIncreaseIsRateOverTheRange(t *testing.T) {
	inc := transpileRange(t, `increase(x{job="j"}[5m])`)
	if !strings.Contains(inc, "c_change / c_span_sec * 300.000000 as val") {
		t.Errorf("increase must scale the slope by the range:\n%s", inc)
	}
	if !strings.Contains(transpileRange(t, `rate(x{job="j"}[5m])`), "c_change / c_span_sec as val") {
		t.Error("rate must not carry the range scaling increase does")
	}
}

// TestCounterResetsStopAtTheRangeBoundary guards the reset correction against
// double counting. The earliest in-range sample compares against a sample outside
// the range, so any drop it records happened before the range began and is not
// part of the change measured inside it. CounterFlagsPlanner applies the same
// correction to its transition counts via first_flag.
func TestCounterResetsStopAtTheRangeBoundary(t *testing.T) {
	for _, fn := range []string{"rate", "increase"} {
		t.Run(fn, func(t *testing.T) {
			got := transpileRange(t, fn+`(x{job="j"}[5m])`)
			if !strings.Contains(got,
				"argMinIf(reset, timestamp_ms, source = 1) OVER cnt_close_wnd as first_reset") {
				t.Errorf("%s: missing the boundary reset:\n%s", fn, got)
			}
			if !strings.Contains(got, "last_v - first_v + (resets - first_reset) as c_change") {
				t.Errorf("%s: boundary reset must be subtracted from the total:\n%s", fn, got)
			}
		})
	}
	// delta has no reset correction at all, so it has nothing to subtract.
	if got := transpileRange(t, `delta(x{job="j"}[5m])`); !strings.Contains(got, "last_v - first_v as c_change") {
		t.Errorf("delta must measure the raw change:\n%s", got)
	}
}

// TestCounterProbesOnlyTheRange guards the removal of the second window. The
// change is measured between the first and last sample inside (t-range, t]; a
// sample before the range is not one of the two endpoints, so probing for one is
// work that no longer feeds the result.
func TestCounterProbesOnlyTheRange(t *testing.T) {
	got := transpileRange(t, `rate(x{job="j"}[5m])`)
	for _, bad := range []string{"cnt_open_wnd", "start_open", "open_cnt", "cnt_start"} {
		if strings.Contains(got, bad) {
			t.Errorf("%q belongs to the pre-range probe, which no longer feeds the value:\n%s", bad, got)
		}
	}
}

// TestFillIsBoundedByStaleness guards the cost of the fill. Pinning it to the
// query window instead pads every series across the whole range regardless of
// its lifetime, which under pod churn is almost entirely rows the source = 1
// gate then discards. STALENESS must also never be paired with FROM: clickhouse
// rejects that combination outright.
func TestFillIsBoundedByStaleness(t *testing.T) {
	// 5m lookback, rendered in ms, matching sum_over_time's own range.
	got := transpileRange(t, `sum_over_time(x{job="j"}[5m])`)
	if !strings.Contains(got, "WITH FILL TO 1700000000000 STEP 60000 STALENESS 300000") {
		t.Errorf("fill must be bounded by staleness, not by the query window:\n%s", got)
	}
	if strings.Contains(got, "WITH FILL FROM") {
		t.Errorf("clickhouse rejects FROM together with STALENESS:\n%s", got)
	}
}

// TestFillArrayJoinOnOldClickHouse guards the fallback for clickhouse < 24.11,
// where WITH FILL STALENESS is a parse error. The fill must instead be an
// arrayJoin range expansion, and STALENESS must not appear anywhere.
func TestFillArrayJoinOnOldClickHouse(t *testing.T) {
	got := transpileRangeCtx(t, `sum_over_time(x{job="j"}[5m])`, rangeTestCtxCap(false))
	if strings.Contains(got, "STALENESS") {
		t.Errorf("STALENESS is a parse error on old clickhouse and must not be emitted:\n%s", got)
	}
	for _, w := range []string{
		"leadInFrame(toInt64(timestamp_ms), 1, toInt64(timestamp_ms) + toInt64(300000))",
		"arrayJoin(range(bucket_ms, least(bucket_ms + toInt64(300000), next_ms, toInt64(1700000000000)), toInt64(60000)))",
		"toUInt8(timestamp_ms = bucket_ms) as source",
	} {
		if !strings.Contains(got, w) {
			t.Errorf("missing %q in arrayJoin fallback:\n%s", w, got)
		}
	}
	// The same leading-edge anchor as the staleness path: read a lookback early.
	if !strings.Contains(got, "1699996100000000000") {
		t.Errorf("arrayJoin fallback must also read a lookback before ctx.From:\n%s", got)
	}
}

// TestFillReadsBeforeFrom guards the leading edge. Dropping FROM means nothing
// pads the window before the first real row, so the first steps are only covered
// because the read reaches back a lookback and leaves a real row there for the
// fill to carry forward from.
func TestFillReadsBeforeFrom(t *testing.T) {
	got := transpileRange(t, `sum_over_time(x{job="j"}[5m])`)
	// ctx.From is 1699996400s; a 5m lookback puts the read start 300s earlier.
	if !strings.Contains(got, "1699996100000000000") {
		t.Errorf("read must start a lookback before ctx.From to anchor the fill:\n%s", got)
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
