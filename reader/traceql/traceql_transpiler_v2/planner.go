package traceql_transpiler_v2

import (
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v4/reader/traceql/tempo"
)

// Plan creates a TraceRequestProcessor from a Tempo AST RootExpr.
// This is the main entry point for the new Tempo-based transpiler.
func Plan(root *tempo.RootExpr) (shared.TraceRequestProcessor, error) {
	planner := &TempoPlanner{root: root}
	sqlPlanner, err := planner.Plan()
	if err != nil {
		return nil, err
	}

	// Extract select attributes and hints
	selectAttrs := planner.extractSelectAttrs()
	mostRecent := planner.extractMostRecentHint()

	return NewRequestProcessorWithProjection(sqlPlanner, selectAttrs, mostRecent), nil
}

// PlanSQL creates a SQL planner from a Tempo AST RootExpr.
// This is used internally and for testing.
func PlanSQL(root *tempo.RootExpr) (shared.SQLRequestPlanner, error) {
	planner := &TempoPlanner{root: root}
	return planner.Plan()
}

// TempoPlanner converts Tempo AST to ClickHouse SQL.
type TempoPlanner struct {
	root *tempo.RootExpr
}

// Plan creates the SQL request planner from the Tempo AST.
func (p *TempoPlanner) Plan() (shared.SQLRequestPlanner, error) {
	// Build the pipeline planner
	pipelinePlanner, err := p.planPipeline(p.root.Pipeline)
	if err != nil {
		return nil, err
	}

	// Wrap with index limit
	result := &IndexLimitPlanner{Main: pipelinePlanner}

	// Extract select attributes for TracesDataPlanner
	selectAttrs := p.extractSelectAttrs()

	// Wrap with traces data planner (final SELECT with trace info)
	result2 := NewTracesDataPlannerWithAttrs(result, selectAttrs)

	// Wrap with another index limit
	result3 := &IndexLimitPlanner{Main: result2}

	return result3, nil
}

// extractSelectAttrs extracts attribute keys from select() operations.
func (p *TempoPlanner) extractSelectAttrs() []string {
	var attrs []string
	for _, elem := range p.root.Pipeline.Elements {
		if sel, ok := elem.(tempo.SelectOperation); ok {
			for _, attr := range sel.Attrs() {
				attrs = append(attrs, attr.String())
			}
		}
	}
	return attrs
}

// extractMostRecentHint checks if with(most_recent=true) hint is set.
func (p *TempoPlanner) extractMostRecentHint() bool {
	if p.root.Hints == nil {
		return false
	}
	if v, ok := p.root.Hints.GetBool(tempo.HintMostRecent, false); ok {
		return v
	}
	return false
}

// planPipeline converts a Tempo Pipeline to a SQL planner.
func (p *TempoPlanner) planPipeline(pipeline tempo.Pipeline) (shared.SQLRequestPlanner, error) {
	if len(pipeline.Elements) == 0 {
		// Empty pipeline {} - match all spans
		return &AllSpansPlanner{}, nil
	}

	// Process pipeline elements
	var conditions []SQLConditionPlanner
	for _, elem := range pipeline.Elements {
		switch e := elem.(type) {
		case *tempo.SpansetFilter:
			condPlanner, err := p.planSpansetFilter(e)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, condPlanner)

		case tempo.SpansetOperation:
			// For now, handle simple spanset operations by processing both sides
			// TODO: Implement proper spanset operations (>>, <<, etc.)
			leftPlanner, err := p.planSpansetExpression(e.LHS)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, leftPlanner)

			rightPlanner, err := p.planSpansetExpression(e.RHS)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, rightPlanner)

		case tempo.SelectOperation:
			// Ignore select() - it doesn't affect filtering
			continue

		case tempo.GroupOperation:
			// Ignore group() for now
			continue

		case tempo.CoalesceOperation:
			// Ignore coalesce() for now
			continue

		case tempo.ScalarFilter:
			// Scalar filters like | count() > 5
			// These require aggregation, handle separately
			condPlanner, err := p.planScalarFilter(e)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, condPlanner)
		}
	}

	if len(conditions) == 0 {
		return &AllSpansPlanner{}, nil
	}

	if len(conditions) == 1 {
		return &SingleConditionPlanner{Condition: conditions[0]}, nil
	}

	return &MultiConditionPlanner{Conditions: conditions, Op: "AND"}, nil
}

// planSpansetExpression handles SpansetExpression nodes.
func (p *TempoPlanner) planSpansetExpression(expr tempo.SpansetExpression) (SQLConditionPlanner, error) {
	switch e := expr.(type) {
	case *tempo.SpansetFilter:
		return p.planSpansetFilter(e)
	case tempo.Pipeline:
		// Nested pipeline
		planner, err := p.planPipeline(e)
		if err != nil {
			return nil, err
		}
		return &WrapperConditionPlanner{Planner: planner}, nil
	case tempo.SpansetOperation:
		// Recursive operation
		left, err := p.planSpansetExpression(e.LHS)
		if err != nil {
			return nil, err
		}
		right, err := p.planSpansetExpression(e.RHS)
		if err != nil {
			return nil, err
		}
		return &BinaryConditionPlanner{Left: left, Right: right, Op: "AND"}, nil
	default:
		return &TrueConditionPlanner{}, nil
	}
}
