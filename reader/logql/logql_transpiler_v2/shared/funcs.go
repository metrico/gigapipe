package shared

import (
	"github.com/metrico/qryn/reader/logql/logql_parser"
	"reflect"
	"time"
)

func GetDuration(script any) (time.Duration, error) {
	dfs := func(node ...any) (time.Duration, error) {
		for _, n := range node {
			if n != nil && !reflect.ValueOf(n).IsNil() {
				res, err := GetDuration(n)
				if err != nil {
					return 0, err
				}
				if res.Nanoseconds() != 0 {
					return res, nil
				}
			}
		}
		return 0, nil
	}

	switch script.(type) {
	case *logql_parser.LogQLScript:
		script := script.(*logql_parser.LogQLScript)
		return dfs(script.AggOperator, script.LRAOrUnwrap, script.TopK, script.QuantileOverTime)
	case *logql_parser.LRAOrUnwrap:
		script := script.(*logql_parser.LRAOrUnwrap)
		return time.ParseDuration(script.Time + script.TimeUnit)
	case *logql_parser.AggOperator:
		return GetDuration(&script.(*logql_parser.AggOperator).LRAOrUnwrap)
	case *logql_parser.TopK:
		script := script.(*logql_parser.TopK)
		return dfs(script.LRAOrUnwrap, script.QuantileOverTime, script.AggOperator)
	case *logql_parser.QuantileOverTime:
		script := script.(*logql_parser.QuantileOverTime)
		return time.ParseDuration(script.Time + script.TimeUnit)
	}
	return 0, nil
}

func GetStrSelector(script any) *logql_parser.StrSelector {
	dfs := func(node ...any) *logql_parser.StrSelector {
		for _, n := range node {
			if n != nil && !reflect.ValueOf(n).IsNil() {
				return GetStrSelector(n)
			}
		}
		return nil
	}

	switch script.(type) {
	case *logql_parser.LogQLScript:
		script := script.(*logql_parser.LogQLScript)
		return dfs(script.StrSelector, script.TopK, script.AggOperator, script.LRAOrUnwrap, script.QuantileOverTime)
	case *logql_parser.StrSelector:
		return script.(*logql_parser.StrSelector)
	case *logql_parser.TopK:
		script := script.(*logql_parser.TopK)
		return dfs(script.QuantileOverTime, script.LRAOrUnwrap, script.AggOperator)
	case *logql_parser.AggOperator:
		script := script.(*logql_parser.AggOperator)
		return dfs(&script.LRAOrUnwrap)
	case *logql_parser.LRAOrUnwrap:
		return &script.(*logql_parser.LRAOrUnwrap).StrSel
	case *logql_parser.QuantileOverTime:
		script := script.(*logql_parser.QuantileOverTime)
		return &script.StrSel
	}
	return nil
}
