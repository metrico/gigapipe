package planner

import (
	"regexp"
	"strings"

	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
)

type LineFilterPlanner struct {
	GenericPlanner
	Op  string
	Val string

	re *regexp.Regexp
}

func (a *LineFilterPlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	if a.Op == "|~" || a.Op == "!~" {
		var err error
		a.re, err = regexp.Compile(a.Val)
		if err != nil {
			return nil, err
		}
	}
	var _entries []shared.LogEntry
	return a.WrapProcess(ctx, in, GenericPlannerOps{
		OnEntry: func(entry *shared.LogEntry) error {
			if entry.Err != nil || a.compare(ctx, entry) {
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

func (a *LineFilterPlanner) compare(_ *shared.PlannerContext,
	in *shared.LogEntry) bool {
	switch a.Op {
	case "|=":
		return strings.Contains(in.Message, a.Val)
	case "!=":
		return !strings.Contains(in.Message, a.Val)
	case "|~":
		return a.re.MatchString(in.Message)
	case "!~":
		return !a.re.MatchString(in.Message)
	}
	return false
}
