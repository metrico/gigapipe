package optimizer

import (
	"github.com/metrico/qryn/v5/reader/promql/promql_transpiler/planner"
	"github.com/prometheus/prometheus/model/labels"
	prom_parser "github.com/prometheus/prometheus/promql/parser"
)

// substituteSelector clones src as the synthetic selector standing in for an
// expression pushed down into ClickHouse.
//
// Clone rather than build fresh: the engine derives the substitute's time window
// from the modifier fields (offset, @, and whatever prometheus adds next), so a
// dropped field is a silently wrong window rather than an error.
//
// @ resolves to 15s granularity, not to the instant: metrics_15s stamps every
// sample with its 15s floor, so a sample landing after the @ instant but inside
// the same bucket is read as though it were at the bucket start.
func substituteSelector(src *prom_parser.VectorSelector, metricName string) *prom_parser.VectorSelector {
	sub := *src
	sub.Name = metricName
	// Load-bearing: the engine re-derives __name__ from Name, so keeping the
	// original matchers breaks the substitute lookup in prom_queryable. Only
	// the grid matcher carries over.
	sub.LabelMatchers = nil
	if g := taggedGrid(src); g != nil {
		sub.LabelMatchers = []*labels.Matcher{g.Matcher()}
	}
	sub.UnexpandedSeriesSet = nil
	sub.Series = nil
	return &sub
}

// taggedGrid returns the grid vs is tagged with, or nil.
func taggedGrid(vs *prom_parser.VectorSelector) *planner.Grid {
	g, _, ok := planner.GridFromMatchers(vs.LabelMatchers)
	if !ok {
		return nil
	}
	return &g
}

// pushable reports whether vs, with range rangeMs (0 for none), may be pushed
// down. An untagged selector may.
func pushable(vs *prom_parser.VectorSelector, rangeMs int64) bool {
	g := taggedGrid(vs)
	return g == nil || g.Pushable(rangeMs)
}

// streamSelect selects the series of vs by its label matchers, the grid
// matcher excluded.
func streamSelect(vs *prom_parser.VectorSelector) *planner.StreamSelectPlanner {
	fp := &planner.StreamSelectPlanner{}
	_, matchers, _ := planner.GridFromMatchers(vs.LabelMatchers)
	for _, m := range matchers {
		fp.LabelNames = append(fp.LabelNames, m.Name)
		fp.Ops = append(fp.Ops, m.Type.String())
		fp.Values = append(fp.Values, m.Value)
	}
	return fp
}
