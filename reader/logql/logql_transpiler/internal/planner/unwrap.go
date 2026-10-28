package planner

import (
	"fmt"
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
		return parseBytes(val)
	default:
		return strconv.ParseFloat(val, 64)
	}
}

func parseBytes(s string) (float64, error) {
	s = strings.ReplaceAll(s, ",", "")
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty bytes value")
	}
	// Find where the numeric part ends.
	i := 0
	for i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '.') {
		i++
	}
	if i == 0 {
		return 0, fmt.Errorf("no numeric value in %q", s)
	}
	num, err := strconv.ParseFloat(s[:i], 64)
	if err != nil {
		return 0, err
	}
	unit := strings.TrimSpace(strings.ToLower(s[i:]))
	switch unit {
	case "", "b":
		return num, nil
	case "kib", "ki":
		return num * 1024, nil
	case "mib", "mi":
		return num * 1048576, nil
	case "gib", "gi":
		return num * 1073741824, nil
	case "tib", "ti":
		return num * 1099511627776, nil
	case "pib", "pi":
		return num * 1125899906842624, nil
	case "eib", "ei":
		return num * 1152921504606846976, nil
	case "kb", "k":
		return num * 1000, nil
	case "mb", "m":
		return num * 1000000, nil
	case "gb", "g":
		return num * 1000000000, nil
	case "tb", "t":
		return num * 1000000000000, nil
	case "pb", "p":
		return num * 1000000000000000, nil
	case "eb", "e":
		return num * 1000000000000000000, nil
	default:
		return 0, fmt.Errorf("unknown bytes unit %q", unit)
	}
}
