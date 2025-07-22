package planner

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/metrico/qryn/reader/logql/parser"
	"github.com/metrico/qryn/reader/logql/transpiler/shared"
)

type LabelFilterPlanner struct {
	GenericPlanner
	Filter *parser.LabelFilter
}

func (a *LabelFilterPlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	fn, err := a.makeFilter(a.Filter)
	if err != nil {
		return nil, err
	}
	var _entries []shared.LogEntry
	return a.WrapProcess(ctx, in, GenericPlannerOps{
		OnEntry: func(entry *shared.LogEntry) error {
			if fn(entry.Labels) {
				_entries = append(_entries, *entry)
			}
			return nil
		},
		OnAfterEntriesSlice: func(entries []shared.LogEntry, c chan []shared.LogEntry) error {
			c <- _entries
			_entries = nil
			return nil
		},
		OnAfterEntries: func(c chan []shared.LogEntry) error {
			return nil
		},
	})
}

func (a *LabelFilterPlanner) makeFilter(filter *parser.LabelFilter) (func(map[string]string) bool, error) {
	var (
		res func(map[string]string) bool
		err error
	)
	if filter.Head.SimpleHead != nil {
		if contains([]string{"=", "=~", "!~"}, filter.Head.SimpleHead.Fn) ||
			(filter.Head.SimpleHead.Fn == "!=" && filter.Head.SimpleHead.StrVal != nil) {
			res, err = a.stringSimpleFilter(filter.Head.SimpleHead)
			if err != nil {
				return nil, err
			}
		} else {
			res, err = a.numberSimpleFilter(filter.Head.SimpleHead)
			if err != nil {
				return nil, err
			}
		}
	} else {
		res, err = a.makeFilter(filter.Head.ComplexHead)
		if err != nil {
			return nil, err
		}
	}
	if filter.Tail == nil {
		return res, nil
	}
	switch strings.ToLower(filter.Op) {
	case "and":
		fn2, err := a.makeFilter(filter.Tail)
		if err != nil {
			return nil, err
		}
		return func(m map[string]string) bool {
			return res(m) && fn2(m)
		}, nil
	case "or":
		fn2, err := a.makeFilter(filter.Tail)
		if err != nil {
			return nil, err
		}
		return func(m map[string]string) bool {
			return res(m) || fn2(m)
		}, nil
	}
	return res, nil
}

func (a *LabelFilterPlanner) stringSimpleFilter(filter *parser.SimpleLabelFilter,
) (func(map[string]string) bool, error) {
	strVal, err := filter.StrVal.Unquote()
	if err != nil {
		return nil, err
	}

	switch filter.Fn {
	case "=":
		return func(m map[string]string) bool {
			return strVal == m[filter.Label.Name]
		}, nil
	case "!=":
		return func(m map[string]string) bool {
			return strVal != m[filter.Label.Name]
		}, nil
	case "=~":
		re, err := regexp.Compile(strVal)
		if err != nil {
			return nil, err
		}
		return func(m map[string]string) bool {
			return re.MatchString(m[filter.Label.Name])
		}, nil
	case "!~":
		re, err := regexp.Compile(strVal)
		if err != nil {
			return nil, err
		}
		return func(m map[string]string) bool {
			return !re.MatchString(m[filter.Label.Name])
		}, nil
	}
	return nil, fmt.Errorf("invalid simple label filter")
}

func (a *LabelFilterPlanner) numberSimpleFilter(filter *parser.SimpleLabelFilter,
) (func(map[string]string) bool, error) {
	iVal, err := strconv.ParseFloat(filter.NumVal, 64)
	if err != nil {
		return nil, err
	}

	var fn func(float64) bool
	switch filter.Fn {
	case ">":
		fn = func(val float64) bool {
			return val > iVal
		}
	case ">=":
		fn = func(val float64) bool {
			return val >= iVal
		}
	case "<":
		fn = func(val float64) bool {
			return val < iVal
		}
	case "<=":
		fn = func(val float64) bool {
			return val <= iVal
		}
	case "==":
		fn = func(val float64) bool {
			return iVal == val
		}
	case "!=":
		fn = func(val float64) bool {
			return iVal != val
		}
	}
	return func(m map[string]string) bool {
		strVal := m[filter.Label.Name]
		if strVal == "" {
			return false
		}
		iVal, err := strconv.ParseFloat(strVal, 64)
		if err != nil {
			return false
		}
		return fn(iVal)
	}, nil
}
