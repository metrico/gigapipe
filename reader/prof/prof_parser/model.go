package prof_parser

import (
	"strconv"
	"strings"
)

type Script struct {
	Selectors []Selector `"{" @@? ("," @@ )* ","? "}" `
}

type Selector struct {
	Name string `@Label_name`
	Op   string `@("="|"!="|"=~"|"!~")`
	Val  Str    `@@`
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
