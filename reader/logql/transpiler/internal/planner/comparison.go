package planner

import "github.com/metrico/qryn/reader/logql/transpiler/shared"

type ComparisonPlanner struct {
	GenericPlanner
	Op  string
	Val float64
}

func (a *ComparisonPlanner) IsMatrix() bool {
	return true
}

func (a *ComparisonPlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	var _entries []shared.LogEntry
	return a.WrapProcess(ctx, in, GenericPlannerOps{
		OnEntry: func(entry *shared.LogEntry) error {
			if a.compare(ctx, entry) {
				_entries = append(_entries, *entry)
			}
			return nil
		},
		OnAfterEntriesSlice: func(entries []shared.LogEntry, out chan []shared.LogEntry) error {
			out <- _entries
			_entries = nil
			return nil
		},
		OnAfterEntries: func(c chan []shared.LogEntry) error {
			return nil
		},
	})
}

func (a *ComparisonPlanner) compare(_ *shared.PlannerContext, e *shared.LogEntry) bool {
	switch a.Op {
	case ">":
		return e.Value > a.Val
	case ">=":
		return e.Value >= a.Val
	case "<":
		return e.Value < a.Val
	case "<=":
		return e.Value <= a.Val
	case "==":
		return e.Value == a.Val
	case "!=":
		return e.Value != a.Val
	}
	return false
}
