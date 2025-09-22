package planner

import (
	"fmt"

	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
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

	return p.WrapProcess(ctx, in, GenericPlannerOps{
		OnEntry: func(entry *shared.LogEntry) error {
			if entry.Err != nil {
				return nil
			}
			var err error
			parser.setLabels(&entry.Labels)
			err = parser.parse(entry.Message)
			if err != nil {
				return err
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
