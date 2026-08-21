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
// Note on @: the accelerated path emits one row per step bucket -- see the
// intDiv(timestamp_ns, step) * step floor in planner/bucket_producer.go -- so an
// @ instant that is not step aligned lands on the enclosing bucket, whose SQL
// row timestamp is up to one step early. (The HTTP response is unaffected: it
// carries the engine's own grid timestamps.) The value that row holds does not go
// stale with it: BucketProducer bounds the read at timestamp_ns <= ctx.To, and
// ctx.To is the instant to the millisecond, so the bucket aggregate covers the
// 15s buckets at or before @ -- to 15s granularity, see below.
//
// Measured, not inferred: against clickhouse 25.3.14.14 on the e2e stack, which
// takes the WITH FILL ... STALENESS path. A counter sampled every 15s, read at a
// 60s step, @ on a bucket boundary and @ 40s and 55s past one all returned the
// same value as stock prometheus 2.53.0 fed the identical samples.
//
// The residual inexactness is the 15s downsample, not the step: metrics_15s
// stamps every sample with its 15s floor, so a raw sample landing after the @
// instant but inside the same 15s bucket is read as though it were at the bucket
// start. Measured: samples at t and t+7s, @ t+3s returned the t+7s value where
// prometheus returned the t value. That is bounded by 15s, and so by one step --
// the accelerated path is only reached for steps of 15s or more (useRawData in
// reader/service/prom_queryable.go).
//
// None of the above was checked on clickhouse < 24.11, where planner/fill_gaps.go
// takes the arrayJoin fallback instead. That path caps at ctx.To with a half-open
// range(), so by inspection the node landing exactly on the instant is never
// emitted. Tracked in #906.
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
