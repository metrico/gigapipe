package planner

import (
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/metrico/qryn/reader/logql/parser"
	"github.com/metrico/qryn/reader/logql/transpiler/shared"
)

func Plan(script *parser.LogQLScript,
	in shared.RequestProcessor) (shared.RequestProcessor, error) {
	strSelector := shared.GetStrSelector(script)
	for _, ppl := range strSelector.Pipelines {
		if ppl.LineFilter != nil {
			str, err := ppl.LineFilter.Val.Unquote()
			if err != nil {
				return nil, err
			}
			in = &LineFilterPlanner{
				GenericPlanner: GenericPlanner{in},
				Op:             ppl.LineFilter.Fn,
				Val:            str,
				re:             nil,
			}
			continue
		}
		if ppl.LabelFormat != nil {
			in = &LabelFormatPlanner{
				GenericPlanner: GenericPlanner{in},
				LabelFormat:    ppl.LabelFormat,
			}
			continue
		}
		if ppl.LabelFilter != nil {
			in = &LabelFilterPlanner{
				GenericPlanner: GenericPlanner{in},
				Filter:         ppl.LabelFilter,
			}
			continue
		}
		if ppl.LineFormat != nil {
			str, err := ppl.LineFormat.Val.Unquote()
			if err != nil {
				return nil, err
			}
			in = &LineFormatterPlanner{
				GenericPlanner: GenericPlanner{in},
				Template:       str,
			}
			continue
		}
		if ppl.Unwrap != nil {
			in = &UnwrapPlanner{
				GenericPlanner: GenericPlanner{in},
				Label:          ppl.Unwrap.Label.Name,
			}
			continue
		}
		if ppl.Parser != nil {
			names := make([]string, len(ppl.Parser.ParserParams))
			vals := make([]string, len(ppl.Parser.ParserParams))
			for i, param := range ppl.Parser.ParserParams {
				var err error
				names[i] = param.Label.Name
				vals[i], err = param.Val.Unquote()
				if err != nil {
					return nil, err
				}
			}
			in = &ParserPlanner{
				GenericPlanner:  GenericPlanner{in},
				Op:              ppl.Parser.Fn,
				ParameterNames:  names,
				ParameterValues: vals,
			}
			continue
		}
		if ppl.Drop != nil {
			names := make([]string, len(ppl.Drop.Params))
			vals := make([]string, len(ppl.Drop.Params))
			for i, param := range ppl.Drop.Params {
				names[i] = param.Label.Name
				var (
					err error
					val string
				)
				if param.Val != nil {
					val, err = param.Val.Unquote()
					if err != nil {
						return nil, err
					}
				}
				vals[i] = val
			}
			in = &DropPlanner{
				GenericPlanner: GenericPlanner{in},
				Labels:         names,
				Values:         vals,
			}
		}
	}
	in, err := planAggregators(script, in)
	if err != nil {
		return nil, err
	}
	if !in.IsMatrix() {
		in = &LimitPlanner{GenericPlanner{in}}
		in = &ResponseOptimizerPlanner{GenericPlanner{in}}
	}
	return in, err
}

func planAggregators(script any, init shared.RequestProcessor) (shared.RequestProcessor, error) {
	dfs := func(node ...any) (shared.RequestProcessor, error) {
		for _, n := range node {
			if n != nil && !reflect.ValueOf(n).IsNil() {
				return planAggregators(n, init)
			}
		}
		return init, nil
	}
	maybeComparison := func(proc shared.RequestProcessor,
		comp *parser.Comparison) (shared.RequestProcessor, error) {
		if comp == nil {
			return proc, nil
		}
		fVal, err := strconv.ParseFloat(comp.Val, 64)
		if err != nil {
			return nil, err
		}
		return &ComparisonPlanner{
			GenericPlanner: GenericPlanner{proc},
			Op:             comp.Fn,
			Val:            fVal,
		}, nil
	}

	switch script := script.(type) {
	case *parser.LogQLScript:
		return dfs(script.TopK, script.AggOperator, script.LRAOrUnwrap, script.QuantileOverTime)
	case *parser.AggOperator:
		proc, err := dfs(&script.LRAOrUnwrap)
		if err != nil {
			return nil, err
		}
		duration, err := time.ParseDuration(script.LRAOrUnwrap.Time + script.LRAOrUnwrap.TimeUnit)
		if err != nil {
			return nil, err
		}
		proc = planByWithout(proc, script.ByOrWithoutPrefix, script.ByOrWithoutSuffix)
		return maybeComparison(&AggOpPlanner{
			AggregatorPlanner: AggregatorPlanner{
				GenericPlanner: GenericPlanner{proc},
				Duration:       duration,
			},
			Func: script.Fn,
		}, script.Comparison)
	case *parser.LRAOrUnwrap:
		duration, err := time.ParseDuration(script.Time + script.TimeUnit)
		if err != nil {
			return nil, err
		}
		var p shared.RequestProcessor
		if len(script.StrSel.Pipelines) > 0 && script.StrSel.Pipelines[len(script.StrSel.Pipelines)-1].Unwrap != nil {
			init = planByWithout(init, script.ByOrWithoutPrefix, script.ByOrWithoutSuffix)
			p = &UnwrapAggPlanner{
				AggregatorPlanner: AggregatorPlanner{
					GenericPlanner: GenericPlanner{init},
					Duration:       duration,
				},
				Function: script.Fn,
			}
		} else {
			p = &LRAPlanner{
				AggregatorPlanner: AggregatorPlanner{
					GenericPlanner: GenericPlanner{init},
					Duration:       duration,
				},
				Func: script.Fn,
			}
		}
		return maybeComparison(p, script.Comparison)
	case *parser.QuantileOverTime:
		return nil, &shared.NotSupportedError{Msg: "quantile_over_time is not supported"}
	case *parser.TopK:
		return nil, &shared.NotSupportedError{Msg: "topk is not supported for the current request"}
	}
	return init, nil
}

func planByWithout(init shared.RequestProcessor,
	byWithout ...*parser.ByOrWithout) shared.RequestProcessor {
	var _byWithout *parser.ByOrWithout
	for _, b := range byWithout {
		if b != nil {
			_byWithout = b
		}
	}

	if _byWithout == nil {
		return init
	}

	labels := make([]string, len(_byWithout.Labels))
	for i, l := range _byWithout.Labels {
		labels[i] = l.Name
	}

	return &ByWithoutPlanner{
		GenericPlanner: GenericPlanner{init},
		By:             strings.ToLower(_byWithout.Fn) == "by",
		Labels:         labels,
	}
}
