package clickhouse_transpiler

import (
	"fmt"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	traceql_parser "github.com/metrico/qryn/v4/reader/traceql/traceql_parser"
)

func Plan(script *traceql_parser.TraceQLScript) (shared.SQLRequestPlanner, error) {
	return (&planner{script: script}).plan()
}

func PlanEval(script *traceql_parser.TraceQLScript) (shared.SQLRequestPlanner, error) {
	return (&planner{script: script}).planEval()
}

func PlanTagsV2(script *traceql_parser.TraceQLScript) (shared.SQLRequestPlanner, error) {
	return (&planner{script: script}).planTagsV2()
}

func PlanValuesV2(script *traceql_parser.TraceQLScript, key string) (shared.SQLRequestPlanner, error) {
	return (&planner{script: script}).planValuesV2(key)
}

type planner struct {
	script *traceql_parser.TraceQLScript
	prefix int

	//Analyze results
}

func (p *planner) plan() (shared.SQLRequestPlanner, error) {
	// Unwrap a single pure-wrapper paren: ({A}) → {A}.
	// Do NOT unwrap when the outer has Op/Tail/MetricsFn — that would lose those nodes.
	if p.script.ParenExpr != nil &&
		p.script.Op == "" && p.script.Tail == nil &&
		p.script.MetricsFn == nil && p.script.WithHints == nil && p.script.SecondStage == nil {
		p.script = p.script.ParenExpr
	}

	var res shared.SQLRequestPlanner
	var err error
	if p.script.Tail == nil {
		res, err = (&simpleExpressionPlanner{script: p.script}).planner()
		if err != nil {
			return nil, err
		}
	} else {
		root := &rootExpressionPlanner{}
		p.planComplex(root, root, p.script)
		res, err = root.planner()
		if err != nil {
			return nil, err
		}
	}
	res = &IndexLimitPlanner{res}

	res = NewTracesDataPlanner(res)

	res = &IndexLimitPlanner{res}

	return res, nil
}

func (p *planner) planTagsV2() (shared.SQLRequestPlanner, error) {
	return (&simpleExpressionPlanner{script: p.script}).tagsV2Planner()
}

func (p *planner) planValuesV2(key string) (shared.SQLRequestPlanner, error) {
	return (&simpleExpressionPlanner{script: p.script}).valuesV2Planner(key)
}

func (p *planner) getPrefix() string {
	p.prefix++
	return fmt.Sprintf("_%d", p.prefix)
}

func (p *planner) planComplex(root iExpressionPlanner, current iExpressionPlanner,
	script *traceql_parser.TraceQLScript) {
	// Unwrap parenthesized expressions
	for script.ParenExpr != nil {
		inner := script.ParenExpr
		if inner.Tail == nil {
			// Atomic inner: safe to promote outer Op/Tail into inner.
			inner.Op = script.Op
			inner.Tail = script.Tail
			script = inner
		} else if script.Op == "" && script.Tail == nil {
			// Complex inner, no outer Op/Tail: pure wrapper, safe to substitute.
			script = inner
		} else {
			// Complex inner with outer Op/Tail: can't safely flatten.
			// Break and let the planner treat the paren group as an opaque selector.
			break
		}
	}
	// For structural operators (&>>, !>>, <<&, <<~, ~), treat as && (flatten structural hierarchy)
	op := script.Op
	switch op {
	case "&>>", "!>>", "<<&", "<<~", "~":
		op = "&&"
	}
	switch op {
	case "":
		current.addOp(&simpleExpressionPlanner{script: script, prefix: p.getPrefix()})
	case "&&":
		current.addOp(&complexExpressionPlanner{
			prefix: p.getPrefix(),
			_fn:    "&&",
			_operands: []iExpressionPlanner{&simpleExpressionPlanner{
				script: script,
				prefix: p.getPrefix(),
			}},
		})
		p.planComplex(root, current.operands()[0], script.Tail)
	case "||":
		current.addOp(&simpleExpressionPlanner{
			script: script,
			prefix: p.getPrefix(),
		})
		root.setOps([]iExpressionPlanner{&complexExpressionPlanner{
			prefix:    p.getPrefix(),
			_fn:       "||",
			_operands: root.operands(),
		}})
		p.planComplex(root, root.operands()[0], script.Tail)
	}
}

func (p *planner) planEval() (shared.SQLRequestPlanner, error) {
	// Unwrap a single pure-wrapper paren: ({A}) → {A}.
	if p.script.ParenExpr != nil &&
		p.script.Op == "" && p.script.Tail == nil &&
		p.script.MetricsFn == nil && p.script.WithHints == nil && p.script.SecondStage == nil {
		p.script = p.script.ParenExpr
	}

	var (
		res shared.SQLRequestPlanner
		err error
	)
	if p.script.Tail == nil {
		res, err = (&simpleExpressionPlanner{script: p.script, prefix: p.getPrefix()}).planEval()
	} else {
		root := &rootExpressionPlanner{}
		p.planComplex(root, root, p.script)
		res, err = root.planEval()
	}
	if err != nil {
		return nil, err
	}
	res = &EvalFinalizerPlanner{Main: res}
	return res, nil
}

type condition struct {
	simpleIdx int // index of term; -1 means complex

	op      string
	complex []*condition
}
