package prof_parser

import (
	"strconv"
	"strings"
)

type Script struct {
	Selectors []Selector `"{" @@? ("," @@ )* ","? "}" `
}

type Selector struct {
	Name LabelName `@(Label_name|Quoted_string)`
	Op   string    `@("="|"!="|"=~"|"!~")`
	Val  Str       `@@`
}

// LabelName is a selector's label name. A name outside [a-zA-Z_][a-zA-Z0-9_]*,
// such as an OTLP attribute name like service.version, is written in double
// quotes as in the Prometheus UTF-8 matcher syntax; the quotes are removed here,
// so the value is always the plain name.
type LabelName string

func (n *LabelName) Capture(values []string) error {
	v := values[0]
	if strings.HasPrefix(v, `"`) {
		u, err := strconv.Unquote(v)
		if err != nil {
			return err
		}
		v = u
	}
	*n = LabelName(v)
	return nil
}

type Str struct {
	Str string `@(Quoted_string|Ticked_string)`
}

func (s Str) Unquote() (string, error) {
	if s.Str[0] == '`' {
		return strings.Trim(s.Str, "`"), nil
	}
	return strconv.Unquote(s.Str)
}
