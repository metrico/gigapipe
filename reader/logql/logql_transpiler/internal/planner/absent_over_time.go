package planner

import (
	"fmt"
	"maps"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_parser"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
)

// AbsentOverTimePlanner emits 1, labelled Labels, at every grid point whose
// window (T-offset-R, T-offset] holds no entry of any series.
type AbsentOverTimePlanner struct {
	GenericPlanner
	Duration time.Duration
	Offset   time.Duration
	Labels   map[string]string
}

func (a *AbsentOverTimePlanner) IsMatrix() bool {
	return true
}

func (a *AbsentOverTimePlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	if ctx.Grid == nil {
		return nil, fmt.Errorf("absent_over_time: no evaluation grid")
	}
	ws, err := newWindows(*ctx.Grid, a.Duration, a.Offset)
	if err != nil {
		return nil, err
	}
	n := make([]float64, ws.buckets)
	return a.GenericPlanner.WrapProcess(ctx, in, GenericPlannerOps{
		OnEntry: func(e *shared.LogEntry) error {
			if e.Err != nil {
				return e.Err
			}
			if i, ok := ws.bucket(e.TimestampNS); ok {
				n[i]++
			}
			return nil
		},
		OnAfterEntriesSlice: func(entries []shared.LogEntry, c chan []shared.LogEntry) error {
			return nil
		},
		OnAfterEntries: func(out chan []shared.LogEntry) error {
			cum := prefixSum(n)
			labels := maps.Clone(a.Labels)
			fp := fingerprint(labels)
			var entries []shared.LogEntry
			for k := int64(0); k < ws.grid.Points; k++ {
				lo, hi := ws.span(k)
				if cum[hi] == cum[lo] {
					entries = append(entries, shared.LogEntry{
						Fingerprint: fp,
						TimestampNS: ws.grid.At(k),
						Labels:      labels,
						Value:       1,
					})
				}
			}
			if len(entries) > 0 {
				out <- entries
			}
			return nil
		},
	})
}

// absentLabels returns the labels of an absent series: the selector's
// equality matchers, without any name that has another matcher.
func absentLabels(cmds []logql_parser.StrSelCmd) (map[string]string, error) {
	labels := map[string]string{}
	var drop []string
	for _, c := range cmds {
		if _, ok := labels[c.Label.Name]; c.Op != "=" || ok {
			drop = append(drop, c.Label.Name)
			continue
		}
		v, err := c.Val.Unquote()
		if err != nil {
			return nil, err
		}
		labels[c.Label.Name] = v
	}
	for _, name := range drop {
		delete(labels, name)
	}
	return labels, nil
}
