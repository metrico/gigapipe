package planner

import (
	"github.com/metrico/qryn/reader/logql/logql_parser"
	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
)

type LabelFormatPlanner struct {
	GenericPlanner
	LabelFormat *logql_parser.LabelFormat
}

func (a *LabelFormatPlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {

	var labelFns []func(map[string]string) map[string]string
	for _, op := range a.LabelFormat.LabelFormatOps {
		label := op.Label.Name

		if op.ConstVal != nil {
			str, err := op.ConstVal.Unquote()
			if err != nil {
				return nil, err
			}

			labelFns = append(labelFns, func(m map[string]string) map[string]string {
				m[label] = str
				return m
			})
			continue
		}

		change := op.LabelVal.Name

		labelFns = append(labelFns, func(m map[string]string) map[string]string {
			val := m[change]
			if val == "" {
				return m
			}
			m[label] = val
			return m
		})
	}

	return a.WrapProcess(ctx, in, GenericPlannerOps{
		OnEntry: func(entry *shared.LogEntry) error {
			for _, fn := range labelFns {
				entry.Labels = fn(entry.Labels)
			}
			return nil
		},
		OnAfterEntriesSlice: func(entries []shared.LogEntry, c chan []shared.LogEntry) error {
			c <- entries
			return nil
		},
		OnAfterEntries: func(c chan []shared.LogEntry) error {
			return nil
		},
	})
}
