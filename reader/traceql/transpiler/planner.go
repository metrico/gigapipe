package traceql_transpiler

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	"github.com/metrico/qryn/reader/model"
	traceql_parser "github.com/metrico/qryn/reader/traceql/parser"
	"github.com/metrico/qryn/reader/traceql/transpiler/clickhouse_transpiler"
)

func Plan(script *traceql_parser.TraceQLScript) (shared.TraceRequestProcessor, error) {
	optimizeScriptSelectors(script)

	var isMetricsReq bool
	traceql_parser.Visit(script, func(node any) error {
		_, ok := node.(*traceql_parser.MetricFunction)
		isMetricsReq = isMetricsReq || ok
		return nil
	})

	if !isMetricsReq {
		return planSamplesReq(script)
	}
	return planMetricsReq(script)
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

func planSamplesReq(script *traceql_parser.TraceQLScript) (shared.TraceRequestProcessor, error) {
	groupBy := clickhouse_transpiler.GetGroupByAttributes(script)

	sqlPlanner, err := clickhouse_transpiler.Plan(script)
	if err != nil {
		return nil, err
	}

	complexityPlanner, err := clickhouse_transpiler.PlanEval(script)
	if err != nil {
		return nil, err
	}

	var res shared.TraceRequestProcessor = &TraceQLSamplesComplexityEvaluator{
		TraceQLComplexityEvaluator[model.TraceInfo]{
			initSqlPlanner:            sqlPlanner,
			simpleRequestProcessor:    &SimpleRequestProcessor{},
			complexRequestProcessor:   &ComplexRequestProcessor{},
			evaluateComplexityPlanner: complexityPlanner,
		},
	}

	if len(groupBy) > 0 {
		res = &GroupByProcessor{
			main:        res,
			groupFields: groupBy,
		}
	}

	return res, nil
}

func planMetricsReq(script *traceql_parser.TraceQLScript) (shared.TraceRequestProcessor, error) {
	sqlPlanner, err := clickhouse_transpiler.Plan(script)
	if err != nil {
		return nil, err
	}

	var fnName string
	traceql_parser.Visit(script, func(node any) error {
		if _fn, ok := node.(*traceql_parser.MetricFunction); ok {
			fnName = _fn.Fn
			return nil
		}
		return nil
	})

	res := &MetricsProcessor{
		main: sqlPlanner,
		fn:   fnName,
	}
	return res, nil
}
