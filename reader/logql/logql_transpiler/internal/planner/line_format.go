package planner

import (
	"bytes"
	"regexp"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig"
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
)

var functionMap = func() template.FuncMap {
	res := template.FuncMap{
		"ToLower":    strings.ToLower,
		"ToUpper":    strings.ToUpper,
		"Replace":    strings.Replace,
		"Trim":       strings.Trim,
		"TrimLeft":   strings.TrimLeft,
		"TrimRight":  strings.TrimRight,
		"TrimPrefix": strings.TrimPrefix,
		"TrimSuffix": strings.TrimSuffix,
		"TrimSpace":  strings.TrimSpace,
		"regexReplaceAll": func(regex string, s string, repl string) string {
			r := regexp.MustCompile(regex)
			return r.ReplaceAllString(s, repl)
		},
		"regexReplaceAllLiteral": func(regex string, s string, repl string) string {
			r := regexp.MustCompile(regex)
			return r.ReplaceAllLiteralString(s, repl)
		},
	}
	sprigFuncMap := sprig.GenericFuncMap()
	for _, addFn := range []string{"lower", "upper", "title", "trunc", "substr", "contains",
		"hasPrefix", "hasSuffix", "indent", "nindent", "replace", "repeat", "trim",
		"trimAll", "trimSuffix", "trimPrefix", "int", "float64", "add", "sub", "mul",
		"div", "mod", "addf", "subf", "mulf", "divf", "max", "min", "maxf", "minf", "ceil", "floor",
		"round", "fromJson", "date", "toDate", "now", "unixEpoch",
	} {
		if function, ok := sprigFuncMap[addFn]; ok {
			res[addFn] = function
		}
	}
	return res
}()

type LineFormatterPlanner struct {
	GenericPlanner
	Template string
}

func (l *LineFormatterPlanner) Process(ctx *shared.PlannerContext,
	in chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	tpl, err := template.New("line").Option("missingkey=zero").Funcs(functionMap).Parse(l.Template)
	if err != nil {
		return nil, err
	}

	var _entries []shared.LogEntry
	i := 0
	return l.WrapProcess(ctx, in, GenericPlannerOps{
		OnEntry: func(entry *shared.LogEntry) error {
			var buf bytes.Buffer
			_labels := make(map[string]string)
			for k, v := range entry.Labels {
				_labels[k] = v
			}
			_labels["_entry"] = entry.Message
			if err := tpl.Execute(&buf, _labels); err != nil {
				return nil
			}
			entry.Message = buf.String()
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
