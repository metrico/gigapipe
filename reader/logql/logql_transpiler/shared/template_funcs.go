package shared

import (
	"regexp"
	"strings"
	"text/template"
	"time"

	"github.com/Masterminds/sprig"
)

type lineFormatBindings struct {
	line string
	ts   time.Time
}

func (b *lineFormatBindings) register(funcs template.FuncMap) {
	funcs["__line__"] = func() string { return b.line }
	funcs["__timestamp__"] = func() time.Time { return b.ts }
}

func (b *lineFormatBindings) bind(entry LogEntry) {
	b.line = entry.Message
	b.ts = time.Unix(0, entry.TimestampNS).UTC()
}

// BaseTemplateFuncs returns the shared LogQL template functions used by line_format
// and label_format, excluding per-entry builtins such as __line__ and __timestamp__.
func BaseTemplateFuncs() template.FuncMap {
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
		"round", "fromJson", "date", "toDate", "toDateInZone", "now", "unixEpoch",
		"duration", "duration_seconds", "len", "eq", "ne", "and", "or", "not",
	} {
		if function, ok := sprigFuncMap[addFn]; ok {
			res[addFn] = function
		}
	}
	if divFn, ok := res["div"]; ok {
		res["divide"] = divFn
	}
	return res
}

// EntryTemplateLabels returns label values available to line_format templates.
func EntryTemplateLabels(entry LogEntry) map[string]string {
	labels := make(map[string]string, len(entry.Labels)+2)
	for k, v := range entry.Labels {
		labels[k] = v
	}
	labels["_entry"] = entry.Message
	labels["__line__"] = entry.Message
	return labels
}

// PrepareLineFormatTemplate parses a line_format/label_format template and returns
// a binder that must be called before each Execute with the current log entry.
func PrepareLineFormatTemplate(templateStr string) (*template.Template, func(LogEntry), error) {
	bindings := &lineFormatBindings{}
	funcs := BaseTemplateFuncs()
	bindings.register(funcs)

	tpl, err := template.New("line").Option("missingkey=zero").Funcs(funcs).Parse(templateStr)
	if err != nil {
		return nil, nil, err
	}

	return tpl, bindings.bind, nil
}

// ExecuteLineFormatTemplate renders a parsed template for a single log entry.
func ExecuteLineFormatTemplate(tpl *template.Template, bind func(LogEntry), entry LogEntry) (string, error) {
	bind(entry)
	var buf strings.Builder
	if err := tpl.Execute(&buf, EntryTemplateLabels(entry)); err != nil {
		return "", err
	}
	return buf.String(), nil
}
