package planner

import "github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"

type KeepPlanner struct {
	GenericPlanner
	Labels []string
	Values []string
}

func (a *KeepPlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {

	return a.WrapProcess(ctx, in, GenericPlannerOps{
		OnEntry: a.filterLabels,
		OnAfterEntriesSlice: func(entries []shared.LogEntry, out chan []shared.LogEntry) error {
			out <- entries
			return nil
		},
		OnAfterEntries: func(c chan []shared.LogEntry) error {
			return nil
		},
	})
}

func (a *KeepPlanner) filterLabels(e *shared.LogEntry) error {
	if e.Labels == nil {
		return nil
	}
	recountFP := false
	for k, v := range e.Labels {
		if isSpecialKeepLabel(k) {
			continue
		}
		if !shouldKeepLabel(k, v, a.Labels, a.Values) {
			delete(e.Labels, k)
			recountFP = true
		}
	}
	if recountFP {
		e.Fingerprint = fingerprint(e.Labels)
	}
	return nil
}

func shouldKeepLabel(k, v string, labels, values []string) bool {
	for i, l := range labels {
		if k != l {
			continue
		}
		if values[i] == "" || values[i] == v {
			return true
		}
	}
	return false
}

func isSpecialKeepLabel(name string) bool {
	return name == shared.ErrorLabel || name == shared.ErrorDetailsLabel
}
