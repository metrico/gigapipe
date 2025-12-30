package planner

import (
	"strconv"
	"strings"
	"time"

	"github.com/metrico/qryn/v4/reader/logql/logql_parser"
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
)

func Plan(script *logql_parser.LogQLScript,
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
	if logql_parser.FindFirst[logql_parser.TopK](script) != nil {
		return nil, &shared.NotSupportedError{Msg: "topk is not supported"}
	}
	if logql_parser.FindFirst[logql_parser.QuantileOverTime](script) != nil {
		return nil, &shared.NotSupportedError{Msg: "quantile_over_time is not supported"}
	}
	maybeComparison := func(proc shared.RequestProcessor,
		comp *logql_parser.Comparison) (shared.RequestProcessor, error) {
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
	var aggOp *logql_parser.AggOperator = logql_parser.FindFirst[logql_parser.AggOperator](script)
	var lra *logql_parser.LRAOrUnwrap = logql_parser.FindFirst[logql_parser.LRAOrUnwrap](script)
	if aggOp == nil && lra == nil {
		return init, nil
	}
	duration, err := time.ParseDuration(lra.Time + lra.TimeUnit)
	if err != nil {
		return nil, err
	}
	proc := init
	hasUnwrap := len(lra.StrSel.Pipelines) > 0 && lra.StrSel.Pipelines[len(lra.StrSel.Pipelines)-1].Unwrap != nil
	if aggOp != nil && !hasUnwrap {
		if canSwapByWithout(aggOp.Fn, lra.Fn) && lra.Comparison == nil {
			if aggOp.ByOrWithoutPrefix == nil && aggOp.ByOrWithoutSuffix == nil {
				proc = planByWithout(proc, &logql_parser.ByOrWithout{Fn: "by", Labels: nil})
			} else {
				proc = planByWithout(proc, aggOp.ByOrWithoutPrefix, aggOp.ByOrWithoutSuffix)
			}
			proc = &LRAPlanner{
				AggregatorPlanner: AggregatorPlanner{
					GenericPlanner: GenericPlanner{proc},
					Duration:       duration,
				},
				Func: lra.Fn,
			}
			return maybeComparison(proc, aggOp.Comparison)
		}
	}

	if len(lra.StrSel.Pipelines) > 0 && lra.StrSel.Pipelines[len(lra.StrSel.Pipelines)-1].Unwrap != nil {
		proc = planByWithout(proc, lra.ByOrWithoutPrefix, lra.ByOrWithoutSuffix)
		proc = &UnwrapAggPlanner{
			AggregatorPlanner: AggregatorPlanner{
				GenericPlanner: GenericPlanner{proc},
				Duration:       duration,
			},
			Function: lra.Fn,
		}
	} else {
		proc = &LRAPlanner{
			AggregatorPlanner: AggregatorPlanner{
				GenericPlanner: GenericPlanner{proc},
				Duration:       duration,
			},
			Func: lra.Fn,
		}
	}
	proc, err = maybeComparison(proc, lra.Comparison)
	if aggOp == nil || err != nil {
		return proc, err
	}

	if aggOp.ByOrWithoutPrefix == nil && aggOp.ByOrWithoutSuffix == nil {
		proc = planByWithout(proc, &logql_parser.ByOrWithout{Fn: "by", Labels: nil})
	} else {
		proc = planByWithout(proc, aggOp.ByOrWithoutPrefix, aggOp.ByOrWithoutSuffix)
	}
	return maybeComparison(&AggOpPlanner{
		AggregatorPlanner: AggregatorPlanner{
			GenericPlanner: GenericPlanner{proc},
			Duration:       duration,
		},
		Func: aggOp.Fn,
	}, aggOp.Comparison)
}

func planByWithout(init shared.RequestProcessor,
	byWithout ...*logql_parser.ByOrWithout) shared.RequestProcessor {
	var _byWithout *logql_parser.ByOrWithout
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

// canSwapByWithout checks if outer aggregation function can commute with inner range function
// allowing us to apply by/without before the inner function to reduce cardinality early
func canSwapByWithout(outerFn string, innerFn string) bool {
	switch outerFn {
	case "sum":
		// sum(by(count)) = by(sum(count)) for count-like operations
		switch innerFn {
		case "count_over_time", "rate", "bytes_over_time", "bytes_rate", "sum_over_time":
			return true
		}
	case "max":
		// max(by(max)) = by(max(max))
		if innerFn == "max_over_time" {
			return true
		}
	case "min":
		// min(by(min)) = by(min(min))
		if innerFn == "min_over_time" {
			return true
		}
	}
	return false
}
