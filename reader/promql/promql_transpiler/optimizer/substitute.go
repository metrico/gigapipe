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
// Note on @: the accelerated path buckets samples on the step grid, so an @
// instant that is not step aligned resolves to the enclosing bucket. The value
// is therefore accurate to within one step, and exact only for step aligned
// instants. The window itself is always exact.
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
