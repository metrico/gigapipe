package traceql_transpiler

import (
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
	"github.com/metrico/qryn/v5/reader/traceql/traceqlconfig"
)

// Plan plans a search for query as a newest-first walk. A query that selects
// every span loads limit traces per slice and has no span budget.
func Plan(query *traceql_ir.Query) (shared.TraceRequestProcessor, error) {
	sqlPlanner, err := clickhouse_transpiler.Plan(query)
	if err != nil {
		return nil, err
	}

	budget := traceqlconfig.PortionSpans()
	if clickhouse_transpiler.AllSpans(query) {
		budget = 0
	}
	return newWalkProcessor(sqlPlanner, budget), nil
}

// PlanTagsV2 plans the attribute keys of the spans a search for query returns.
func PlanTagsV2(query *traceql_ir.Query) (shared.GenericTraceRequestProcessor[string], error) {
	plan, err := clickhouse_transpiler.PlanTagsV2(query)
	if err != nil {
		return nil, err
	}
	return planAutocomplete(query, plan)
}

// PlanValuesV2 plans the values of attribute key of the spans a search for
// query returns.
func PlanValuesV2(query *traceql_ir.Query, key string) (shared.GenericTraceRequestProcessor[string], error) {
	plan, err := clickhouse_transpiler.PlanValuesV2(query, key)
	if err != nil {
		return nil, err
	}
	return planAutocomplete(query, plan)
}

// planAutocomplete reads plan, by complexity portions when the query's spans
// are costly to evaluate. A plan without ids statements reads at once.
func planAutocomplete(query *traceql_ir.Query, plan *clickhouse_transpiler.SearchPlan) (
	shared.GenericTraceRequestProcessor[string], error) {
	if len(plan.Ids) == 0 {
		return &autocompleteRequestProcessor{plan: plan}, nil
	}
	complexityPlanner, err := clickhouse_transpiler.PlanEval(query)
	if err != nil {
		return nil, err
	}
	return &TraceQLComplexityEvaluator{
		main:                      plan,
		simpleRequestProcessor:    &autocompleteRequestProcessor{},
		complexRequestProcessor:   &complexAutocompleteRequestProcessor{},
		evaluateComplexityPlanner: complexityPlanner,
		threshold:                 traceqlconfig.PortionSpans(),
	}, nil
}
