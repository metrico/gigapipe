package traceql_transpiler

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/reader/model"
	"github.com/metrico/qryn/reader/traceql/traceql_parser"
	"github.com/metrico/qryn/reader/traceql/traceql_transpiler/clickhouse_transpiler"
)

func Plan(script *traceql_parser.TraceQLScript) (shared.TraceRequestProcessor, error) {
	sqlPlanner, err := clickhouse_transpiler.Plan(script)
	if err != nil {
		return nil, err
	}

	complexityPlanner, err := clickhouse_transpiler.PlanEval(script)
	if err != nil {
		return nil, err
	}

	return &TraceQLComplexityEvaluator[model.TraceInfo]{
		initSqlPlanner:            sqlPlanner,
		simpleRequestProcessor:    &SimpleRequestProcessor{},
		complexRequestProcessor:   &ComplexRequestProcessor{},
		evaluateComplexityPlanner: complexityPlanner,
	}, nil
}

func PlanTagsV2(script *traceql_parser.TraceQLScript) (shared.GenericTraceRequestProcessor[string], error) {
	if script == nil {
		return &allTagsV2RequestProcessor{}, nil
	}
	res, err := clickhouse_transpiler.PlanTagsV2(script)
	if err != nil {
		return nil, err
	}

	complexityPlanner, err := clickhouse_transpiler.PlanEval(script)
	if err != nil {
		return nil, err
	}

	return &TraceQLComplexityEvaluator[string]{
		initSqlPlanner:            res,
		simpleRequestProcessor:    &SimpleTagsV2RequestProcessor{},
		complexRequestProcessor:   &ComplexTagsV2RequestProcessor{},
		evaluateComplexityPlanner: complexityPlanner,
	}, nil
}

func PlanValuesV2(script *traceql_parser.TraceQLScript, key string) (shared.GenericTraceRequestProcessor[string], error) {
	if script == nil {
		return &allTagsV2RequestProcessor{}, nil
	}
	res, err := clickhouse_transpiler.PlanValuesV2(script, key)
	if err != nil {
		return nil, err
	}

	complexityPlanner, err := clickhouse_transpiler.PlanEval(script)
	if err != nil {
		return nil, err
	}

	return &TraceQLComplexityEvaluator[string]{
		initSqlPlanner:            res,
		simpleRequestProcessor:    &SimpleTagsV2RequestProcessor{},
		complexRequestProcessor:   &ComplexValuesV2RequestProcessor{},
		evaluateComplexityPlanner: complexityPlanner,
	}, nil
}
