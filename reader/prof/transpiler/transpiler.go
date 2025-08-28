package transpiler

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/reader/prof/parser"
	shared2 "github.com/metrico/qryn/reader/prof/shared"
	v1 "github.com/metrico/qryn/reader/prof/types/v1"
)

func PlanLabelNames(scripts []*parser.Script) (shared.SQLRequestPlanner, error) {
	var fpPlanner shared.SQLRequestPlanner
	if len(scripts) > 0 {
		fpPlanner = &UnionAllPlanner{streamSelectorPlanners(scripts)}
	}
	return &LabelNamesPlanner{GenericLabelsPlanner{fpPlanner}}, nil
}

func PlanLabelValues(scripts []*parser.Script, labelName string) (shared.SQLRequestPlanner, error) {
	var fpPlanner shared.SQLRequestPlanner
	if len(scripts) > 0 {
		fpPlanner = &UnionAllPlanner{streamSelectorPlanners(scripts)}
	}
	return &LabelValuesPlanner{GenericLabelsPlanner{fpPlanner}, labelName}, nil
}

func PlanMergeTraces(script *parser.Script, tId *shared2.TypeId) (shared.SQLRequestPlanner, error) {
	_script := *script
	populateTypeId(&_script, tId)
	fpPlanners := streamSelectorPlanners([]*parser.Script{&_script})
	fpPlanner := fpPlanners[0]
	var planner shared.SQLRequestPlanner = &MergeRawPlanner{
		Fingerprints: fpPlanner,
		selectors:    _script.Selectors,
		sampleType:   tId.SampleType,
		sampleUnit:   tId.SampleUnit,
	}
	planner = &MergeJoinedPlanner{planner}
	planner = &MergeAggregatedPlanner{Main: planner}
	return planner, nil
}

func PlanSelectSeries(script *parser.Script, tId *shared2.TypeId, groupBy []string,
	agg v1.TimeSeriesAggregationType, step int64) (shared.SQLRequestPlanner, error) {
	_script := *script
	populateTypeId(&_script, tId)
	fpPlanners := streamSelectorPlanners([]*parser.Script{script})
	labelPlanner := &GetLabelsPlanner{
		FP:        fpPlanners[0],
		GroupBy:   groupBy,
		Selectors: _script.Selectors,
	}
	planner := &SelectSeriesPlanner{
		GetLabelsPlanner: labelPlanner,
		Selectors:        _script.Selectors,
		SampleType:       tId.SampleType,
		SampleUnit:       tId.SampleUnit,
		Aggregation:      agg,
		Step:             step,
	}
	return planner, nil
}

func PlanMergeProfiles(script *parser.Script, tId *shared2.TypeId) (shared.SQLRequestPlanner, error) {
	_script := *script
	populateTypeId(&_script, tId)
	fpPlanners := streamSelectorPlanners([]*parser.Script{script})
	planner := &MergeProfilesPlanner{
		Fingerprints: fpPlanners[0],
		Selectors:    _script.Selectors,
	}
	return planner, nil
}

func PlanSeries(scripts []*parser.Script, labelNames []string) (shared.SQLRequestPlanner, error) {
	selectorsCount := 0
	for _, s := range scripts {
		selectorsCount += len(s.Selectors)
	}
	if selectorsCount == 0 {
		return &AllTimeSeriesSelectPlanner{}, nil
	}
	fpPlanners := streamSelectorPlanners(scripts)
	planners := make([]shared.SQLRequestPlanner, len(fpPlanners))
	for i, fpPlanner := range fpPlanners {
		planners[i] = &TimeSeriesSelectPlanner{
			Fp:        fpPlanner,
			Selectors: scripts[i].Selectors,
		}
	}
	var planner shared.SQLRequestPlanner
	if len(planners) == 1 {
		planner = planners[0]
	} else {
		planner = &UnionAllPlanner{Mains: planners}
		planner = &TimeSeriesDistinctPlanner{Main: planner}
	}
	if len(labelNames) > 0 {
		planner = &FilterLabelsPlanner{Main: planner, Labels: labelNames}
	}
	return planner, nil
}

func PlanAnalyzeQuery(script *parser.Script) (shared.SQLRequestPlanner, error) {
	fpPlanners := streamSelectorPlanners([]*parser.Script{script})
	var planner shared.SQLRequestPlanner = &MergeProfilesPlanner{
		Fingerprints: fpPlanners[0],
		Selectors:    script.Selectors,
	}
	planner = &ProfileSizePlanner{
		Main: planner,
	}
	return planner, nil
}

func populateTypeId(script *parser.Script, tId *shared2.TypeId) {
	script.Selectors = append(script.Selectors, []parser.Selector{
		{Name: "__name__", Op: "=", Val: parser.Str{Str: "`" + tId.Tp + "`"}},
		{Name: "__period_type__", Op: "=", Val: parser.Str{Str: "`" + tId.PeriodType + "`"}},
		{Name: "__period_unit__", Op: "=", Val: parser.Str{Str: "`" + tId.PeriodUnit + "`"}},
		{Name: "__sample_type__", Op: "=", Val: parser.Str{Str: "`" + tId.SampleType + "`"}},
		{Name: "__sample_unit__", Op: "=", Val: parser.Str{Str: "`" + tId.SampleUnit + "`"}},
	}...)
}

func streamSelectorPlanners(scripts []*parser.Script) []shared.SQLRequestPlanner {
	planners := make([]shared.SQLRequestPlanner, len(scripts))
	for i, script := range scripts {
		planners[i] = &StreamSelectorPlanner{script.Selectors}
	}
	return planners
}
