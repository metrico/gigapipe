package clickhouse_transpiler

import (
	"fmt"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
)

// Plan plans a search for query in two statements.
func Plan(query *traceql_ir.Query) (*SearchPlan, error) {
	return (&planner{query: query}).plan()
}

func PlanEval(query *traceql_ir.Query) (shared.SQLRequestPlanner, error) {
	return (&planner{query: query}).planEval()
}

// PlanTagsV2 plans the attribute keys of the spans a search for query
// returns, or of every span in range when AllSpans(query).
func PlanTagsV2(query *traceql_ir.Query) (shared.SQLRequestPlanner, error) {
	if AllSpans(query) {
		return &AllTagsRequestPlanner{}, nil
	}
	return planAutocomplete(query, func(result shared.SQLRequestPlanner, prefix string) shared.SQLRequestPlanner {
		return &SelectTagsPlanner{Result: result, Prefix: prefix}
	})
}

// PlanValuesV2 plans the values of attribute key of the spans a search for
// query returns, or of every span in range when AllSpans(query).
func PlanValuesV2(query *traceql_ir.Query, key string) (shared.SQLRequestPlanner, error) {
	if AllSpans(query) {
		return &AllValuesRequestPlanner{Key: key}, nil
	}
	return planAutocomplete(query, func(result shared.SQLRequestPlanner, prefix string) shared.SQLRequestPlanner {
		return &SelectValuesPlanner{Result: result, Prefix: prefix, Key: key}
	})
}

// planAutocomplete plans the result spans of query and reads them with attrs.
func planAutocomplete(query *traceql_ir.Query,
	attrs func(result shared.SQLRequestPlanner, prefix string) shared.SQLRequestPlanner) (shared.SQLRequestPlanner, error) {
	p := &planner{query: query}
	result, err := p.planResult(false)
	if err != nil {
		return nil, err
	}
	return p.structuralGate(attrs(result, p.getPrefix())), nil
}

// AllSpans reports whether query selects every span: no query, or a lone
// `{}`, select() stages aside.
func AllSpans(query *traceql_ir.Query) bool {
	if query == nil {
		return true
	}
	var elements []traceql_ir.Element
	for _, e := range query.Pipeline.Elements {
		if _, ok := e.(*traceql_ir.Select); !ok {
			elements = append(elements, e)
		}
	}
	if len(elements) == 0 {
		return true
	}
	selector, ok := loneSelector(&traceql_ir.Pipeline{Elements: elements})
	return ok && selector.Filter == nil
}

// loneSelector returns the selector a one-element pipeline is, parenthesised or not.
func loneSelector(pipeline *traceql_ir.Pipeline) (*traceql_ir.Selector, bool) {
	if len(pipeline.Elements) != 1 {
		return nil, false
	}
	expr, ok := pipeline.Elements[0].(traceql_ir.SpansetExpr)
	for ok {
		group, isGroup := expr.(*traceql_ir.Group)
		if !isGroup {
			break
		}
		expr = group.Expr
	}
	selector, ok := expr.(*traceql_ir.Selector)
	return selector, ok
}

type planner struct {
	query  *traceql_ir.Query
	prefix int
}

func (p *planner) plan() (*SearchPlan, error) {
	selected, err := spanValueKeys(p.query)
	if err != nil {
		return nil, err
	}
	res, err := p.planResult(true)
	if err != nil {
		return nil, err
	}
	search := &searchRowsPlanner{Main: &IndexLimitPlanner{&SearchSpansetPlanner{Main: res}}}
	return &SearchPlan{
		Search: p.structuralGate(search),
		Data:   &IndexLimitPlanner{NewTracesDataPlanner(searchResultPlanner{}, selected)},
	}, nil
}

func (p *planner) planEval() (shared.SQLRequestPlanner, error) {
	var res shared.SQLRequestPlanner
	if selector, ok := loneSelector(&p.query.Pipeline); ok {
		op := &simpleExpressionPlanner{selector: selector, prefix: p.getPrefix(), operand: selector.Filter != nil}
		var err error
		if res, err = op.planEval(); err != nil {
			return nil, err
		}
	} else {
		operands, err := p.pipelineEvals(&p.query.Pipeline)
		if err != nil {
			return nil, err
		}
		res = &ComplexEvalOrPlanner{Operands: operands, Prefix: p.getPrefix()}
	}
	return &EvalFinalizerPlanner{Main: res}, nil
}

// planSpansetExpr plans a spanset operator tree into span rows.
func (p *planner) planSpansetExpr(expr traceql_ir.SpansetExpr) (shared.SQLRequestPlanner, error) {
	return planSpansetTree(p, expr,
		func(op *simpleExpressionPlanner) (shared.SQLRequestPlanner, error) { return op.planner() },
		p.planPipeline,
		func(op traceql_ir.SpansetOperator, lhs, rhs shared.SQLRequestPlanner) shared.SQLRequestPlanner {
			switch op {
			case traceql_ir.OpAnd:
				return &TraceAndPlanner{LHS: lhs, RHS: rhs, Prefix: p.getPrefix()}
			case traceql_ir.OpOr:
				return &TraceOrPlanner{LHS: lhs, RHS: rhs, Prefix: p.getPrefix()}
			}
			if op.IsOneHop() {
				return &OneHopPlanner{Op: op, LHS: lhs, RHS: rhs, Prefix: p.getPrefix()}
			}
			return &TransitivePlanner{Op: op, LHS: lhs, RHS: rhs, Prefix: p.getPrefix()}
		})
}

// pipelineEvals estimates the cost of every operand the pipeline reads: those
// of its spanset expressions, or every span in range for a leading scalar filter.
func (p *planner) pipelineEvals(pipeline *traceql_ir.Pipeline) ([]shared.SQLRequestPlanner, error) {
	var res []shared.SQLRequestPlanner
	if readsEverySpan(pipeline) {
		res = append(res, &AllSpansEvaluatorPlanner{Prefix: p.getPrefix()})
	}
	for _, element := range pipeline.Elements {
		if e, ok := element.(traceql_ir.SpansetExpr); ok {
			operands, err := planSpansetTree(p, e,
				func(op *simpleExpressionPlanner) ([]shared.SQLRequestPlanner, error) {
					res, err := op.planEval()
					return []shared.SQLRequestPlanner{res}, err
				},
				p.pipelineEvals,
				func(_ traceql_ir.SpansetOperator, lhs, rhs []shared.SQLRequestPlanner) []shared.SQLRequestPlanner {
					return append(lhs, rhs...)
				})
			if err != nil {
				return nil, err
			}
			res = append(res, operands...)
		}
	}
	return res, nil
}

// planSpansetTree folds a spanset operator tree: leaf plans each operand
// selector, pipeline each parenthesised pipeline, and join combines the two
// sides of an operator. Groups plan as their content.
func planSpansetTree[T any](p *planner, expr traceql_ir.SpansetExpr,
	leaf func(*simpleExpressionPlanner) (T, error),
	pipeline func(*traceql_ir.Pipeline) (T, error),
	join func(traceql_ir.SpansetOperator, T, T) T) (T, error) {
	switch expr := expr.(type) {
	case *traceql_ir.Group:
		return planSpansetTree(p, expr.Expr, leaf, pipeline, join)
	case *traceql_ir.Pipeline:
		return pipeline(expr)
	case *traceql_ir.SpansetOp:
		var zero T
		lhs, err := planSpansetTree(p, expr.LHS, leaf, pipeline, join)
		if err != nil {
			return zero, err
		}
		rhs, err := planSpansetTree(p, expr.RHS, leaf, pipeline, join)
		if err != nil {
			return zero, err
		}
		return join(expr.Op, lhs, rhs), nil
	}
	return leaf(p.operand(expr.(*traceql_ir.Selector)))
}

// operand plans one selector of a spanset expression.
func (p *planner) operand(selector *traceql_ir.Selector) *simpleExpressionPlanner {
	return &simpleExpressionPlanner{selector: selector, prefix: p.getPrefix(), operand: true}
}

// structuralGate wraps res in StructuralQueryPlanner when the query has
// structural operators.
func (p *planner) structuralGate(res shared.SQLRequestPlanner) shared.SQLRequestPlanner {
	if p.query.HasStructural() {
		return &StructuralQueryPlanner{Main: res}
	}
	return res
}

func (p *planner) getPrefix() string {
	p.prefix++
	return fmt.Sprintf("_%d", p.prefix)
}

type condition struct {
	simpleIdx int // index of term; -1 means complex

	op      string
	complex []*condition
}
