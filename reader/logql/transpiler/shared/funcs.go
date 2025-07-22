package shared

import (
	"reflect"
	"time"

	"github.com/metrico/qryn/reader/logql/parser"
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

	switch script := script.(type) {
	case *parser.LogQLScript:
		return dfs(script.AggOperator, script.LRAOrUnwrap, script.TopK, script.QuantileOverTime)
	case *parser.LRAOrUnwrap:
		return time.ParseDuration(script.Time + script.TimeUnit)
	case *parser.AggOperator:
		return GetDuration(&script.LRAOrUnwrap)
	case *parser.TopK:
		return dfs(script.LRAOrUnwrap, script.QuantileOverTime, script.AggOperator)
	case *parser.QuantileOverTime:
		return time.ParseDuration(script.Time + script.TimeUnit)
	}
	return 0, nil
}

func GetStrSelector(script any) *parser.StrSelector {
	dfs := func(node ...any) *parser.StrSelector {
		for _, n := range node {
			if n != nil && !reflect.ValueOf(n).IsNil() {
				return GetStrSelector(n)
			}
		}
		return nil
	}

	switch script := script.(type) {
	case *parser.LogQLScript:
		return dfs(script.StrSelector, script.TopK, script.AggOperator, script.LRAOrUnwrap, script.QuantileOverTime)
	case *parser.StrSelector:
		return script
	case *parser.TopK:
		return dfs(script.QuantileOverTime, script.LRAOrUnwrap, script.AggOperator)
	case *parser.AggOperator:
		return dfs(&script.LRAOrUnwrap)
	case *parser.LRAOrUnwrap:
		return &script.StrSel
	case *parser.QuantileOverTime:
		return &script.StrSel
	}
	return nil
}
