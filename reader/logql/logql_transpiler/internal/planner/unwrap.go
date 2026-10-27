package planner

import (
	"strconv"
	"strings"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
)

type UnwrapPlanner struct {
	GenericPlanner
	Label    string
	ConvFunc string
}

func (l *UnwrapPlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	return l.WrapProcess(ctx, in, GenericPlannerOps{
		OnEntry: func(entry *shared.LogEntry) error {
			if entry.Err != nil {
				return nil
			}
			var val string
			if l.Label == "_entry" {
				val = entry.Message
			} else {
				val = entry.Labels[l.Label]
			}
			if val != "" {
				fVal, err := unwrapConvert(val, l.ConvFunc)
				if err != nil {
					return errSkipEntry
				}
				entry.Value = fVal
			} else {
				return errSkipEntry
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

func unwrapConvert(val string, convFunc string) (float64, error) {
	switch convFunc {
	case "duration", "duration_seconds":
		d, err := time.ParseDuration(val)
		if err != nil {
			return 0, err
		}
		return d.Seconds(), nil
	case "bytes":
		return parseBytes(val), nil
	default:
		return strconv.ParseFloat(val, 64)
	}
}

func parseBytes(s string) float64 {
	s = strings.TrimSpace(s)
	// Find where the numeric part ends.
	i := 0
	for i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '.') {
		i++
	}
	num, err := strconv.ParseFloat(s[:i], 64)
	if err != nil {
		return 0
	}
	unit := strings.TrimSpace(strings.ToLower(s[i:]))
	switch unit {
	case "kib":
		return num * 1024
	case "mib":
		return num * 1048576
	case "gib":
		return num * 1073741824
	case "tib":
		return num * 1099511627776
	case "pib":
		return num * 1125899906842624
	case "kb":
		return num * 1000
	case "mb":
		return num * 1000000
	case "gb":
		return num * 1000000000
	case "tb":
		return num * 1000000000000
	case "pb":
		return num * 1000000000000000
	default:
		return num
	}
}
