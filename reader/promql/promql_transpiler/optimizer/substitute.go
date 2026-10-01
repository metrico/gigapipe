package optimizer

import (
	prom_parser "github.com/prometheus/prometheus/promql/parser"
)

// substituteSelector clones src as the synthetic selector standing in for an
// expression pushed down into ClickHouse.
//
// Clone rather than build fresh: the engine derives the substitute's time window
// from the modifier fields (offset, @, and whatever prometheus adds next), so a
// dropped field is a silently wrong window rather than an error.
func substituteSelector(src *prom_parser.VectorSelector, metricName string) *prom_parser.VectorSelector {
	sub := *src
	sub.Name = metricName
	// Load-bearing: the engine re-derives __name__ from Name, so keeping the
	// original matchers breaks the substitute lookup in prom_queryable.
	sub.LabelMatchers = nil
	sub.UnexpandedSeriesSet = nil
	sub.Series = nil
	return &sub
}
