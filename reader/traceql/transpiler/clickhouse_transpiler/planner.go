package clickhouse_transpiler

import (
	"fmt"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
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
	termIdx []*traceql_parser.AttrSelector
	cond    *condition
	aggFn   string
	aggAttr string
	cmpVal  string

	selectAttrs    []traceql_parser.LabelName
	groupByAttrs   []traceql_parser.LabelName
	metricFunction *traceql_parser.MetricFunction

	terms map[string]int
}

func (p *planner) plan() (shared.SQLRequestPlanner, error) {
	var err error
	err = p.analyzeSelectors()
	if err != nil {
		return nil, err
	}

	err = p.analyzeMetricFunction()
	if err != nil {
		return nil, err
	}
	if p.metricFunction == nil {
		return p.planSamplesReq()
	}
	return p.planMerticsReq()
}
func (p *planner) planSamplesReq() (shared.SQLRequestPlanner, error) {
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

	res = NewTracesDataPlanner(res, p.selectAttrs)

	res = &IndexLimitPlanner{res}

	return res, nil
}

func (p *planner) planMerticsReq() (shared.SQLRequestPlanner, error) {
	var res shared.SQLRequestPlanner
	var err error
	res, err = (&simpleExpressionPlanner{script: p.script}).planner()
	if err != nil {
		return nil, err
	}
	res = &MetricPlanner{
		Main: res,
		Fn:   p.metricFunction.Fn,
	}
	return res, nil
}

func (p *planner) planTagsV2() (shared.SQLRequestPlanner, error) {
	return (&simpleExpressionPlanner{script: p.script}).tagsV2Planner()
}

func (p *planner) planValuesV2(key string) (shared.SQLRequestPlanner, error) {
	return (&simpleExpressionPlanner{script: p.script}).valuesV2Planner(key)
}

func (p *planner) analyzeSelectors() error {
	err := traceql_parser.Visit(p.script, func(node any) error {
		if _selector, ok := node.(*traceql_parser.Selector); ok {
			for i := len(_selector.Pipeline) - 1; i >= 0; i-- {
				if _selector.Pipeline[i].Selector != nil {
					p.selectAttrs = append(p.selectAttrs, _selector.Pipeline[i].Selector.Attributes...)
					copy(_selector.Pipeline[i:], _selector.Pipeline[i+1:])
					_selector.Pipeline = _selector.Pipeline[:len(_selector.Pipeline)-1]
					continue
				}
				if _selector.Pipeline[i].By != nil {
					p.selectAttrs = append(p.selectAttrs, _selector.Pipeline[i].By.Attributes...)
					p.groupByAttrs = append(p.groupByAttrs, _selector.Pipeline[i].By.Attributes...)
					copy(_selector.Pipeline[i:], _selector.Pipeline[i+1:])
					_selector.Pipeline = _selector.Pipeline[:len(_selector.Pipeline)-1]
					continue
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func GetGroupByAttributes(script *traceql_parser.TraceQLScript) []traceql_parser.LabelName {
	var res []traceql_parser.LabelName
	traceql_parser.Visit(script, func(node any) error {
		if _selector, ok := node.(*traceql_parser.Selector); ok {
			for _, pipeline := range _selector.Pipeline {
				if pipeline.By != nil {
					res = append(res, pipeline.By.Attributes...)
				}
			}
		}
		return nil
	})
	return res
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

func (p *planner) analyzeMetricFunction() error {
	metricFunc := 0
	err := traceql_parser.Visit(p.script, func(node any) error {
		if f, ok := node.(*traceql_parser.MetricFunction); ok {
			metricFunc++
			if metricFunc > 1 {
				return fmt.Errorf("multiple metric functions are not supported")
			}
			p.metricFunction = f
		}
		return nil
	})
	if err != nil {
		return err
	}
	if metricFunc == 0 {
		return nil
	}
	if p.script.Tail != nil {
		return fmt.Errorf("complex expression with metric functions is not supported")
	}
	return nil
}

type condition struct {
	simpleIdx int // index of term; -1 means complex

	op      string
	complex []*condition
}
