package planner

import (
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
)

// NewInstantVectorPlanner builds the per-series, per-step values a prometheus
// instant vector selector yields: at each step the last real sample within the
// staleness window (t-5m, t], carried forward on the step grid.
//
// That is exactly last_over_time(metric[5m]), so it reuses OverTimePlanner: a
// BucketProducer read, densified to the step grid by FillGapsPlanner sized to the
// staleness delta, then argMaxIf(last, ts, source = 1) over the (t-5m, t] frame.
// It is the producer a cross-series aggregation (sum, avg, ...) must sit on so
// that every series alive at a step contributes there, rather than only the ones
// that happen to have a raw sample landing on that exact step.
func NewInstantVectorPlanner(fp shared.SQLRequestPlanner) shared.SQLRequestPlanner {
	return &OverTimePlanner{FpPlanner: fp, Duration: staleness, Fn: "last_over_time"}
}
