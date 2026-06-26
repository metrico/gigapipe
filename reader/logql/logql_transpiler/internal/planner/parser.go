package planner

import (
	"fmt"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
)

type parserHelper interface {
	parse(line string) error
	setLabels(*map[string]string)
}

type ParserPlanner struct {
	GenericPlanner
	Op              string
	ParameterNames  []string
	ParameterValues []string

	parameterTypedValues [][]string
}

func (p *ParserPlanner) IsMatrix() bool { return false }

func (p *ParserPlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {

	p.parameterTypedValues = make([][]string, len(p.ParameterValues))
	for i, v := range p.ParameterValues {
		var err error
		p.parameterTypedValues[i], err = shared.JsonPathParamToTypedArray(v)
		if err != nil {
			return nil, err
		}
	}

	var parser parserHelper
	switch p.Op {
	case "json":
		if len(p.ParameterNames) > 0 {
			parser = &parameterJsonHelper{
				paths: p.parameterTypedValues,
				keys:  p.ParameterNames,
			}
		} else {
			parser = &plainJsonParserHelper{}
		}
	case "logfmt":
		if len(p.ParameterNames) > 0 {
			parser = &parameterLogfmtHelper{
				keys:  p.ParameterNames,
				paths: p.ParameterValues,
			}
		} else {
			parser = &plainLogfmtHelper{}
		}
	default:
		return nil, &shared.NotSupportedError{Msg: fmt.Sprintf("%s not supported", p.Op)}
	}

	errType := shared.ParserErrorType[p.Op]

	return p.WrapProcess(ctx, in, GenericPlannerOps{
		OnEntry: func(entry *shared.LogEntry) error {
			if entry.Err != nil {
				return nil
			}
			parser.setLabels(&entry.Labels)
			if err := parser.parse(entry.Message); err != nil {
				// A failed parse does not abort the query: flag the entry with
				// the synthetic error labels and keep whatever was extracted
				// before the failure, matching Loki.
				if entry.Labels == nil {
					entry.Labels = map[string]string{}
				}
				entry.Labels[shared.ErrorLabel] = errType
				entry.Labels[shared.ErrorDetailsLabel] = err.Error()
			}
			entry.Fingerprint = fingerprint(entry.Labels)
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
