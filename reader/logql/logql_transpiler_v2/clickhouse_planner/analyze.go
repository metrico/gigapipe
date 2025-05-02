package clickhouse_planner

import (
	"github.com/metrico/qryn/reader/logql/logql_parser"
	"reflect"
	"time"
)

func (p *planner) analyzeScript() {
	pipeline := getPipeline(p.script)

	p.labelsJoinIdx = -1

	offset := analyzeOffset(p.script)
	if offset != nil {
		p.offsetModifier = offset
	}

	p.metrics15Shortcut = AnalyzeMetrics15sShortcut(p.script)
	if p.metrics15Shortcut {
		return
	}

	p.simpleLabelOperation = make([]bool, len(pipeline))
	for i, ppl := range pipeline {
		if ppl.LabelFilter != nil {
			p.simpleLabelOperation[i] = true
		}
		if ppl.Parser != nil {
			break
		}
	}

	for i, ppl := range pipeline {
		if ppl.Parser != nil {
			p.labelsJoinIdx = i
			break
		}
		if ppl.LabelFilter != nil && !p.simpleLabelOperation[i] {
			p.labelsJoinIdx = i
			break
		}
		if ppl.LineFormat != nil {
			p.labelsJoinIdx = i
			break
		}
		if ppl.Drop != nil {
			p.labelsJoinIdx = i
			break
		}
	}

	p.renewMainAfter = make([]bool, len(pipeline))
	for i, ppl := range pipeline {
		p.renewMainAfter[i] = i < len(pipeline)-1 &&
			ppl.Parser != nil && pipeline[i+1].Parser == nil
	}

	for _, ppl := range pipeline {
		p.fastUnwrap = p.fastUnwrap && ppl.Parser == nil
	}

	p.matrixFunctionsLabelsIDX = -1
	p.getFunctionOrder(p.script)

}

func analyzeOffset(script *logql_parser.LogQLScript) *time.Duration {
	offset := findFirst[logql_parser.Offset](script)
	if offset == nil {
		return nil
	}
	d, _ := offset.Duration()
	return &d
}

func AnalyzeMetrics15sShortcut(script *logql_parser.LogQLScript) bool {
	var lraOrUnwrap = findFirst[logql_parser.LRAOrUnwrap](script)
	if lraOrUnwrap == nil {
		return false
	}
	if lraOrUnwrap.Fn != "rate" && lraOrUnwrap.Fn != "count_over_time" {
		return false
	}
	duration, err := time.ParseDuration(lraOrUnwrap.Time + lraOrUnwrap.TimeUnit)
	if err != nil {
		return false
	}
	if duration.Seconds() < 15 {
		return false
	}
	if lraOrUnwrap.StrSel.Pipelines != nil &&
		lraOrUnwrap.StrSel.Pipelines[len(lraOrUnwrap.StrSel.Pipelines)-1].Unwrap != nil {
		return false
	}
	for _, ppl := range lraOrUnwrap.StrSel.Pipelines {
		if ppl.Parser != nil {
			return false
		}
		if ppl.Drop != nil {
			return false
		}
		if ppl.LineFilter != nil {
			str, err := ppl.LineFilter.Val.Unquote()
			if str != "" || err != nil {
				return false
			}
		}
	}
	return true
}

func (p *planner) getFunctionOrder(script any) {
	maybeComparison := func(op *logql_parser.Comparison) {
		if op != nil {
			p.matrixFunctionsOrder = append(p.matrixFunctionsOrder, func() error {
				return p.planComparison(op)
			})
		}
	}

	switch script.(type) {
	case *logql_parser.LogQLScript:
		script := script.(*logql_parser.LogQLScript)
		visit(p.getFunctionOrder, script.LRAOrUnwrap, script.AggOperator, script.TopK, script.QuantileOverTime)
		break
	case *logql_parser.LRAOrUnwrap:
		script := script.(*logql_parser.LRAOrUnwrap)
		if len(script.StrSel.Pipelines) > 0 && script.StrSel.Pipelines[len(script.StrSel.Pipelines)-1].Unwrap != nil {
			if p.matrixFunctionsLabelsIDX == -1 {
				p.matrixFunctionsLabelsIDX = len(p.matrixFunctionsOrder)
			}
			p.matrixFunctionsOrder = append(p.matrixFunctionsOrder, func() error {
				return p.planUnwrapFn(script)
			})
		} else {
			p.matrixFunctionsOrder = append(p.matrixFunctionsOrder, func() error {
				return p.planLRA(script)
			})
		}
		maybeComparison(script.Comparison)
		break
	case *logql_parser.AggOperator:
		script := script.(*logql_parser.AggOperator)
		p.getFunctionOrder(&script.LRAOrUnwrap)
		if script.ByOrWithoutPrefix != nil || script.ByOrWithoutSuffix != nil && p.matrixFunctionsLabelsIDX == -1 {
			p.matrixFunctionsLabelsIDX = len(p.matrixFunctionsOrder)
		}
		p.matrixFunctionsOrder = append(p.matrixFunctionsOrder, func() error {
			return p.planAgg(script, p.matrixFunctionsLabelsIDX != -1)
		})
		maybeComparison(script.Comparison)
		break
	case *logql_parser.TopK:
		script := script.(*logql_parser.TopK)
		visit(p.getFunctionOrder, script.LRAOrUnwrap, script.AggOperator, script.QuantileOverTime)
		p.matrixFunctionsOrder = append(p.matrixFunctionsOrder, func() error {
			return p.planTopK(script)
		})
		maybeComparison(script.Comparison)
		break
	case *logql_parser.QuantileOverTime:
		script := script.(*logql_parser.QuantileOverTime)
		p.matrixFunctionsOrder = append(p.matrixFunctionsOrder, func() error {
			return p.planQuantileOverTime(script)
		})
		maybeComparison(script.Comparison)
		break
	}
}

func visit(fn func(any), nodes ...any) {
	for _, n := range nodes {
		if n != nil && !reflect.ValueOf(n).IsNil() {
			fn(n)
		}
	}
}

func findFirst[T any](nodes ...any) *T {
	for _, n := range nodes {
		if n == nil || reflect.ValueOf(n).IsNil() {
			continue
		}
		if _, ok := n.(*T); ok {
			return n.(*T)
		}
		var res *T
		switch n.(type) {
		case *logql_parser.LogQLScript:
			_n := n.(*logql_parser.LogQLScript)
			res = findFirst[T](
				_n.LRAOrUnwrap,
				_n.AggOperator,
				_n.TopK,
				_n.QuantileOverTime,
				_n.StrSelector,
				_n.Macros,
			)
		case *logql_parser.StrSelector:
			_n := n.(*logql_parser.StrSelector)
			var children []any
			for i := range _n.Pipelines {
				children = append(children, &_n.Pipelines[i])
			}
			for i := range _n.StrSelCmds {
				children = append(children, &_n.StrSelCmds[i])
			}
			res = findFirst[T](children...)
		case *logql_parser.LRAOrUnwrap:
			_n := n.(*logql_parser.LRAOrUnwrap)
			res = findFirst[T](
				&_n.StrSel,
				_n.ByOrWithoutPrefix,
				_n.ByOrWithoutSuffix,
				_n.Comparison,
				_n.Offset,
			)
		case *logql_parser.AggOperator:
			_n := n.(*logql_parser.AggOperator)
			res = findFirst[T](_n.ByOrWithoutPrefix, &_n.LRAOrUnwrap, _n.ByOrWithoutSuffix, _n.Comparison)
		case *logql_parser.MacrosOp:
			_n := n.(*logql_parser.MacrosOp)
			res = findFirst[T](_n.Params)
		case *logql_parser.TopK:
			_n := n.(*logql_parser.TopK)
			res = findFirst[T](_n.LRAOrUnwrap, _n.AggOperator, _n.Comparison, _n.QuantileOverTime)
		case *logql_parser.QuantileOverTime:
			_n := n.(*logql_parser.QuantileOverTime)
			res = findFirst[T](&_n.StrSel, _n.Comparison, _n.ByOrWithoutPrefix, _n.ByOrWithoutSuffix)
		case *logql_parser.StrSelCmd:
			_n := n.(*logql_parser.StrSelCmd)
			res = findFirst[T](&_n.Val, &_n.Label)
		case *logql_parser.StrSelectorPipeline:
			_n := n.(*logql_parser.StrSelectorPipeline)
			res = findFirst[T](
				_n.Parser,
				_n.LineFilter,
				_n.Unwrap,
				_n.LabelFilter,
				_n.LineFormat,
				_n.LabelFormat,
				_n.Drop,
			)
		case *logql_parser.ByOrWithout:
			_n := n.(*logql_parser.ByOrWithout)
			children := make([]any, len(_n.Labels))
			for i := range _n.Labels {
				children[i] = &_n.Labels[i]
			}
			res = findFirst[T](children...)
		}
		if res != nil {
			return res
		}
	}
	return nil
}
