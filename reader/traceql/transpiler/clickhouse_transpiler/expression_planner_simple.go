package clickhouse_transpiler

import (
	"fmt"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	traceql_parser "github.com/metrico/qryn/reader/traceql/parser"
)

type simpleExpressionPlanner struct {
	script *traceql_parser.TraceQLScript
	prefix string

	//Analyze results
	termIdx []*traceql_parser.AttrSelector
	cond    *condition

	agg     *traceql_parser.Aggregator
	aggFn   string
	aggAttr string
	cmpVal  string

	terms map[string]int
}

func (p *simpleExpressionPlanner) addOp(selector iExpressionPlanner) {
}

func (p *simpleExpressionPlanner) setOps(selector []iExpressionPlanner) {
}

func (p *simpleExpressionPlanner) fn() string {
	return ""
}

func (p *simpleExpressionPlanner) operands() []iExpressionPlanner {
	return nil
}

func (p *simpleExpressionPlanner) tagsV2Planner() (shared.SQLRequestPlanner, error) {
	if p.script.Tail != nil {
		return nil, fmt.Errorf("complex requests `{} || {} ...` are not supported")
	}
	err := p.check()
	if err != nil {
		return nil, err
	}

	p.analyze()

	var res shared.SQLRequestPlanner = &AttrConditionPlanner{
		Main:           NewInitIndexPlanner(false),
		Terms:          p.termIdx,
		Conds:          p.cond,
		AggregatedAttr: p.aggAttr,
	}
	res = &SelectTagsPlanner{Main: res}
	return res, nil
}

func (p *simpleExpressionPlanner) valuesV2Planner(key string) (shared.SQLRequestPlanner, error) {
	if p.script.Tail != nil {
		return nil, fmt.Errorf("complex requests `{} || {} ...` are not supported")
	}
	err := p.check()
	if err != nil {
		return nil, err
	}

	p.analyze()

	if p.cond == nil {
		return &AllValuesRequestPlanner{Key: key}, nil
	}

	var res shared.SQLRequestPlanner = &AttrConditionPlanner{
		Main:           NewInitIndexPlanner(false),
		Terms:          p.termIdx,
		Conds:          p.cond,
		AggregatedAttr: p.aggAttr,
	}
	res = &SelectValuesRequestPlanner{
		SelectTagsPlanner: SelectTagsPlanner{Main: res},
		Key:               key,
	}
	return res, nil
}

func (p *simpleExpressionPlanner) planner() (shared.SQLRequestPlanner, error) {
	err := p.check()
	if err != nil {
		return nil, err
	}

	p.analyze()
	var res shared.SQLRequestPlanner
	if p.script.Head.AttrSelector != nil {
		res = &AttrConditionPlanner{
			Main:           NewInitIndexPlanner(false),
			Terms:          p.termIdx,
			Conds:          p.cond,
			AggregatedAttr: p.aggAttr,
		}
	} else {
		res = NewAttrlessConditionPlanner()
	}

	res = &IndexGroupByPlanner{Main: res, Prefix: p.prefix}

	if p.aggFn != "" {
		res = &AggregatorPlanner{
			Main:       res,
			Fn:         p.aggFn,
			Attr:       p.aggAttr,
			CompareFn:  p.agg.Cmp,
			CompareVal: p.cmpVal,
			Prefix:     p.prefix,
		}
	}
	return res, nil
}

func (p *simpleExpressionPlanner) planEval() (shared.SQLRequestPlanner, error) {
	err := p.check()
	if err != nil {
		return nil, err
	}

	p.analyze()
	var res shared.SQLRequestPlanner
	if p.script.Head.AttrSelector != nil {
		res = &AttrConditionEvaluatorPlanner{
			Main: &AttrConditionPlanner{
				Main:           NewInitIndexPlanner(true),
				Terms:          p.termIdx,
				Conds:          p.cond,
				AggregatedAttr: p.aggAttr,
			},
			Prefix: p.prefix,
		}
	} else {
		res = &AttrlessEvaluatorPlanner{
			Prefix: p.prefix,
		}
	}

	return res, nil
}

func (p *simpleExpressionPlanner) check() error {
	if p.script.Head.AttrSelector == nil {
		err := traceql_parser.Visit(&p.script.Head, func(node any) error {
			if _, ok := node.(*traceql_parser.Aggregator); ok {
				return fmt.Errorf("requests like `{} | ....` are not supported")
			}
			return nil
		})
		if err != nil {
			return err
		}
		if p.script.Tail != nil {
			return fmt.Errorf("requests like `{} || .....` are not supported")
		}
	}
	tail := p.script.Tail
	for tail != nil {
		if tail.Head.AttrSelector == nil {
			return fmt.Errorf("requests like `... || {}` are not supported")
		}
		tail = tail.Tail
	}
	return nil
}

func (p *simpleExpressionPlanner) analyze() {
	p.terms = make(map[string]int)
	p.cond = p.analyzeCond(p.script.Head.AttrSelector)
	p.analyzeAgg()
}

func (p *simpleExpressionPlanner) analyzeCond(exp *traceql_parser.AttrSelectorExp) *condition {
	var res *condition
	if exp == nil {
		return nil
	}
	if exp.ComplexHead != nil {
		res = p.analyzeCond(exp.ComplexHead)
	} else if exp.Head != nil {
		term := exp.Head.String()
		if p.terms[term] != 0 {
			res = &condition{simpleIdx: p.terms[term] - 1}
		} else {
			p.termIdx = append(p.termIdx, exp.Head)
			p.terms[term] = len(p.termIdx)
			res = &condition{simpleIdx: len(p.termIdx) - 1}
		}
	}
	if exp.Tail != nil {
		res = &condition{
			simpleIdx: -1,
			op:        exp.AndOr,
			complex:   []*condition{res, p.analyzeCond(exp.Tail)},
		}
	}
	return res
}

func (p *simpleExpressionPlanner) analyzeAgg() {
	traceql_parser.Visit(&p.script.Head, func(node any) error {
		if agg, ok := node.(*traceql_parser.Aggregator); ok {
			p.agg = agg
			p.aggFn = agg.Fn
			p.aggAttr = agg.Attr
			p.cmpVal = agg.Num + agg.Measurement
		}
		return nil
	})
}
