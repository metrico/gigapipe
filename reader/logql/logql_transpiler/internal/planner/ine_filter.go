package planner

import (
	"fmt"
	"regexp"
	"strings"

	logql_parser "github.com/metrico/qryn/v4/reader/logql/logql_parser"
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
)

type LineFilterPlanner struct {
	GenericPlanner
	Filter *logql_parser.LineFilter
}

func (a *LineFilterPlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	eval, err := buildLineFilterEval(a.Filter)
	if err != nil {
		return nil, err
	}
	var _entries []shared.LogEntry
	return a.WrapProcess(ctx, in, GenericPlannerOps{
		OnEntry: func(entry *shared.LogEntry) error {
			if entry.Err != nil || eval(entry.Message) {
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

type lineEval func(msg string) bool

func buildLineFilterEval(lf *logql_parser.LineFilter) (lineEval, error) {
	return buildExpEval(lf.Fn, &lf.Exp)
}

func buildExpEval(fn string, exp *logql_parser.LineFilterExp) (lineEval, error) {
	head, err := buildHeadEval(fn, &exp.Head)
	if err != nil {
		return nil, err
	}
	if exp.Op == "" {
		return head, nil
	}
	tail, err := buildExpEval(fn, exp.Tail)
	if err != nil {
		return nil, err
	}
	if exp.Op == "or" {
		return func(msg string) bool { return head(msg) || tail(msg) }, nil
	}
	return func(msg string) bool { return head(msg) && tail(msg) }, nil
}

func buildHeadEval(fn string, head *logql_parser.LineFilterHead) (lineEval, error) {
	if head.Complex != nil {
		return buildExpEval(fn, head.Complex)
	}
	return buildSimpleEval(fn, head.Simple)
}

func buildSimpleEval(fn string, s *logql_parser.LineFilterSimple) (lineEval, error) {
	val, err := s.Val.Unquote()
	if err != nil {
		return nil, err
	}
	switch fn {
	case "|=":
		return func(msg string) bool { return strings.Contains(msg, val) }, nil
	case "!=":
		return func(msg string) bool { return !strings.Contains(msg, val) }, nil
	case "|~":
		re, err := regexp.Compile(val)
		if err != nil {
			return nil, err
		}
		return re.MatchString, nil
	case "!~":
		re, err := regexp.Compile(val)
		if err != nil {
			return nil, err
		}
		return func(msg string) bool { return !re.MatchString(msg) }, nil
	}
	return nil, fmt.Errorf("unsupported line filter op: %s", fn)
}
