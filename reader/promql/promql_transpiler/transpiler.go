package promql_transpiler

import (
	logql_transpiler_shared "github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v4/reader/model"
	"github.com/metrico/qryn/v4/reader/promql/promql_transpiler/planner"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

type TranspileResponse struct {
	MapResult func(samples []model.Sample) []model.Sample
	Query     sql.ISelect
}

func TranspileLabelMatchers(hints *storage.SelectHints,
	ctx *logql_transpiler_shared.PlannerContext, matchers ...*labels.Matcher) (*TranspileResponse, error) {
	var p logql_transpiler_shared.SQLRequestPlanner = &planner.ValuesPlanner{Fp: streamSelect(matchers...)}
	p = &planner.HintsPlanner{Main: p, Hints: hints}
	p = &planner.LabelsPlanner{p}
	query, err := p.Process(ctx)
	return &TranspileResponse{nil, query}, err
}

func TranspileLabelMatchersDownsample(hints *storage.SelectHints,
	ctx *logql_transpiler_shared.PlannerContext, matchers ...*labels.Matcher) (*TranspileResponse, error) {
	var p logql_transpiler_shared.SQLRequestPlanner = &planner.DownsampleValuesPlanner{
		ValuesPlanner: planner.ValuesPlanner{
			Fp: streamSelect(matchers...),
		},
	}
	p = &planner.DownsampleHintsPlanner{Main: p, Hints: hints}
	p = &planner.LabelsPlanner{p}
	query, err := p.Process(ctx)
	return &TranspileResponse{nil, query}, err
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
