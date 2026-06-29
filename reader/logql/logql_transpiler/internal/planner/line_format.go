package planner

import (
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
)

type LineFormatterPlanner struct {
	GenericPlanner
	Template string
}

func (l *LineFormatterPlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	tpl, bind, err := shared.PrepareLineFormatTemplate(l.Template)
	if err != nil {
		return nil, err
	}

	var _entries []shared.LogEntry
	i := 0
	return l.WrapProcess(ctx, in, GenericPlannerOps{
		OnEntry: func(entry *shared.LogEntry) error {
			message, err := shared.ExecuteLineFormatTemplate(tpl, bind, *entry)
			if err != nil {
				return err
			}
			entry.Message = message
			_entries = append(_entries, *entry)
			return nil
		},
		OnAfterEntriesSlice: func(entries []shared.LogEntry, c chan []shared.LogEntry) error {
			i += 100
			c <- _entries
			_entries = make([]shared.LogEntry, 0, 100)
			return nil
		},
		OnAfterEntries: func(c chan []shared.LogEntry) error {
			return nil
		},
	})
}
