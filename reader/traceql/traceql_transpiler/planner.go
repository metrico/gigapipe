package traceql_transpiler

import (
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
)

func Plan(query *traceql_ir.Query) (shared.TraceRequestProcessor, error) {
	sqlPlanner, err := clickhouse_transpiler.Plan(query)
	if err != nil {
		return nil, err
	}

	complexityPlanner, err := clickhouse_transpiler.PlanEval(query)
	if err != nil {
		return nil, err
	}

	return &TraceQLComplexityEvaluator[model.TraceInfo, *clickhouse_transpiler.SearchPlan]{
		main:                      sqlPlanner,
		simpleRequestProcessor:    &SimpleRequestProcessor{},
		complexRequestProcessor:   &ComplexRequestProcessor{},
		evaluateComplexityPlanner: complexityPlanner,
	}, nil
}

// PlanTagsV2 plans the attribute keys of the spans a search for query returns.
func PlanTagsV2(query *traceql_ir.Query) (shared.GenericTraceRequestProcessor[string], error) {
	main, err := clickhouse_transpiler.PlanTagsV2(query)
	if err != nil {
		return nil, err
	}
	return planAutocomplete(query, main)
}

// PlanValuesV2 plans the values of attribute key of the spans a search for
// query returns.
func PlanValuesV2(query *traceql_ir.Query, key string) (shared.GenericTraceRequestProcessor[string], error) {
	main, err := clickhouse_transpiler.PlanValuesV2(query, key)
	if err != nil {
		return nil, err
	}
	return planAutocomplete(query, main)
}

// planAutocomplete reads main, by complexity portions when the query's spans
// are costly to evaluate. A query that selects every span reads main at once.
func planAutocomplete(query *traceql_ir.Query, main shared.SQLRequestPlanner) (
	shared.GenericTraceRequestProcessor[string], error) {
	if clickhouse_transpiler.AllSpans(query) {
		return &autocompleteRequestProcessor{main: main}, nil
	}
	complexityPlanner, err := clickhouse_transpiler.PlanEval(query)
	if err != nil {
		return nil, err
	}
	return &TraceQLComplexityEvaluator[string, shared.SQLRequestPlanner]{
		main:                      main,
		simpleRequestProcessor:    &autocompleteRequestProcessor{},
		complexRequestProcessor:   &complexAutocompleteRequestProcessor{},
		evaluateComplexityPlanner: complexityPlanner,
	}, nil
}
