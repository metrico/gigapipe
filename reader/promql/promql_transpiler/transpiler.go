package promql_transpiler

import (
	logql_transpiler_shared "github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/promql/promql_transpiler/planner"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

type TranspileResponse struct {
	MapResult func(samples []model.Sample) []model.Sample
	Query     sql.ISelect
	Route     Route
}

// TranspileLabelMatchers plans a raw read. grid, when set, is the selector's
// evaluation grid.
func TranspileLabelMatchers(hints *storage.SelectHints, ctx *logql_transpiler_shared.PlannerContext,
	grid *planner.Grid, matchers ...*labels.Matcher) (*TranspileResponse, error) {
	var p logql_transpiler_shared.SQLRequestPlanner = &planner.ValuesPlanner{Fp: streamSelect(matchers...)}
	p = &planner.HintsPlanner{Main: p, Hints: hints, Grid: grid}
	p = &planner.LabelsPlanner{Main: p}
	query, err := p.Process(ctx)
	return &TranspileResponse{Query: query, Route: RouteRaw}, err
}

// TranspileLabelMatchersDownsample plans a metrics_15s read. grid, when set,
// is the selector's evaluation grid; functions that need distinct samples keep
// the epoch keys.
func TranspileLabelMatchersDownsample(hints *storage.SelectHints, ctx *logql_transpiler_shared.PlannerContext,
	grid *planner.Grid, matchers ...*labels.Matcher) (*TranspileResponse, error) {
	var p logql_transpiler_shared.SQLRequestPlanner
	if grid != nil && !distinctKeyed(hints) {
		p = &planner.DownsampleGridPlanner{Fp: streamSelect(matchers...), Hints: hints, Grid: *grid}
	} else {
		p = &planner.DownsampleHintsPlanner{
			Main:  &planner.DownsampleValuesPlanner{ValuesPlanner: planner.ValuesPlanner{Fp: streamSelect(matchers...)}},
			Hints: hints,
		}
	}
	p = &planner.LabelsPlanner{Main: p}
	query, err := p.Process(ctx)
	return &TranspileResponse{Query: query, Route: RouteMetrics15s}, err
}

func streamSelect(matchers ...*labels.Matcher) logql_transpiler_shared.SQLRequestPlanner {
	fp := &planner.StreamSelectPlanner{}
	for _, matcher := range matchers {
		fp.LabelNames = append(fp.LabelNames, matcher.Name)
		fp.Ops = append(fp.Ops, matcher.Type.String())
		fp.Values = append(fp.Values, matcher.Value)
	}
	return fp
}
