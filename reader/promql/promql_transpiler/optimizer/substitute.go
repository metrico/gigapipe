package optimizer

import (
	prom_parser "github.com/prometheus/prometheus/promql/parser"
)

// substituteSelector clones src as the synthetic selector that stands in for an
// expression pushed down into ClickHouse.
//
// Cloning rather than building a fresh node keeps every evaluation modifier the
// engine needs in order to compute the substitute's time window -- offset, @,
// and whatever prometheus adds next. The engine derives hints.Start/End from
// these fields (promql/engine.go, getTimeRangesForSelector) and
// reader/service/prom_queryable.go turns those hints into PlannerContext
// From/To, so a modifier dropped here becomes a silently wrong query window
// rather than an error.
//
// Note on @: the accelerated path buckets samples on the step grid -- see the
// intDiv(timestamp_ns, step) * step floor in planner/bucket_producer.go -- so an
// @ instant that is not step aligned resolves to the enclosing bucket. The value
// is therefore accurate to within one step.
//
// The step aligned case is not exact either on clickhouse < 24.11: the arrayJoin
// fallback in planner/fill_gaps.go caps at ctx.To with a half-open range(), so
// the node landing exactly on the instant is never emitted. Tracked in #906.
//
// The queried end is exact: hints.End, and so PlannerContext.To, lands on the
// instant to the millisecond. The start is not, but for a reason unrelated to @:
// reader/service/prom_queryable.go floors hints.Start to a 15s grid unless
// COMPAT_4_0_19 is set, for every accelerated query with or without a modifier,
// and derives From as hints.Start - hints.Range (always a plain floor here,
// since a substitute selector carries no range).
func substituteSelector(src *prom_parser.VectorSelector, metricName string) *prom_parser.VectorSelector {
	sub := *src
	sub.Name = metricName
	// The engine re-derives the __name__ matcher from Name. Carrying the
	// original matchers would leave __name__ pointing at the real metric and
	// break the substitute lookup in prom_queryable.transpileLabelMatchers.
	sub.LabelMatchers = nil
	// Populated by the engine at query preparation time; never carried across.
	sub.UnexpandedSeriesSet = nil
	sub.Series = nil
	return &sub
}
