package ruler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/metrico/qryn/v4/reader/promql/promql_parser"
	"github.com/metrico/qryn/v4/reader/service"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
)

// PromEvaluator evaluates PromQL recording-rule expressions as instant queries,
// mirroring the reader's /api/v1/query path.
type PromEvaluator struct {
	queryEngine promql.QueryEngine
	storage     *service.CLokiQueriable
}

// NewPromEvaluator builds a PromQL evaluator over the reader's query engine and
// CLokiQueriable storage.
func NewPromEvaluator(queryEngine promql.QueryEngine, storage *service.CLokiQueriable) *PromEvaluator {
	return &PromEvaluator{queryEngine: queryEngine, storage: storage}
}

// Evaluate runs expr as an instant query at t and returns the result vector.
// Scalar results are wrapped in a single unlabelled sample.
func (e *PromEvaluator) Evaluate(ctx context.Context, expr string, t time.Time) (promql.Vector, error) {
	if expr == "" {
		return nil, errors.New("rule expression cannot be empty")
	}

	parsed, err := promql_parser.Parse(expr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse PromQL expression: %w", err)
	}

	queryable := e.storage.SetOidAndDB(ctx, parsed)
	promQuery, err := e.queryEngine.NewInstantQuery(ctx, queryable, nil, parsed.Expr.String(), t)
	if err != nil {
		return nil, fmt.Errorf("failed to create instant query: %w", err)
	}

	res := promQuery.Exec(ctx)
	if res.Err != nil {
		return nil, fmt.Errorf("query execution failed: %w", res.Err)
	}

	switch v := res.Value.(type) {
	case promql.Vector:
		return v, nil
	case promql.Scalar:
		return promql.Vector{{T: v.T, F: v.V, Metric: labels.EmptyLabels()}}, nil
	default:
		return nil, fmt.Errorf("rule expression returned unexpected type: %T", res.Value)
	}
}
