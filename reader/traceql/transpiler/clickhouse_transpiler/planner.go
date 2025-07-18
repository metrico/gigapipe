package clickhouse_transpiler

import (
	"fmt"

	"github.com/metrico/qryn/reader/logql/transpiler/shared"
	traceql_parser "github.com/metrico/qryn/reader/traceql/parser"
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
	switch script.AndOr {
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
