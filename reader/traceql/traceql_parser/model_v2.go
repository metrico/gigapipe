package traceql_parser

import (
	"encoding/json"
	"strings"
)

type TraceQLScript struct {
	Head  Selector       `@@`
	AndOr string         `@(And|Or)?`
	Tail  *TraceQLScript `@@?`
}

func (l TraceQLScript) String() string {
	var tail string
	if l.AndOr != "" {
		tail = " " + l.AndOr + " " + l.Tail.String()
	}
	return l.Head.String() + tail
}

type Selector struct {
	AttrSelector *AttrSelectorExp `"{" @@? "}"`
	Aggregator   *Aggregator      `@@?`
}

func (s Selector) String() string {
	res := "{" + s.AttrSelector.String() + "}"
	if s.Aggregator != nil {
		res += " " + s.Aggregator.String()
	}
	return res
}

type AttrSelectorExp struct {
	Head        *AttrSelector    `(@@`
	ComplexHead *AttrSelectorExp `| "(" @@ ")" )`
	AndOr       string           `@(And|Or)?`
	Tail        *AttrSelectorExp `@@?`
}

func (a AttrSelectorExp) String() string {
	res := ""
	if a.Head != nil {
		res += a.Head.String()
	}
	if a.ComplexHead != nil {
		res += "(" + a.ComplexHead.String() + ")"
	}
	if a.AndOr != "" {
		res += " " + a.AndOr + " " + a.Tail.String()
	}
	return res
}

type Aggregator struct {
	Fn          string `"|" @("count"|"sum"|"min"|"max"|"avg")`
	Attr        string `"(" @Label_name? ")"`
	Cmp         string `@("="|"!="|"<"|"<="|">"|">=")`
	Num         string `@Minus? @Integer @Dot? @Integer?`
	Measurement string `@("ns"|"us"|"ms"|"s"|"m"|"h"|"d")?`
}

func (a Aggregator) String() string {
	return "| " + a.Fn + "(" + a.Attr + ") " + a.Cmp + " " + a.Num + a.Measurement
}

type AttrSelector struct {
	Label string `@Label_name`
	Op    string `@("="|"!="|"<"|"<="|">"|">="|"=~"|"!~")`
	Val   Value  `@@`
}

func (a AttrSelector) String() string {
	return a.Label + " " + a.Op + " " + a.Val.String()
}

type Value struct {
	TimeVal string        `@Integer @Dot? @Integer? @("ns"|"us"|"ms"|"s"|"m"|"h"|"d")`
	FVal    string        `| @Minus? @Integer @Dot? @Integer?`
	StrVal  *QuotedString `| @@`
}

func (v Value) String() string {
	if v.StrVal != nil {
		return v.StrVal.Str
	}
	if v.FVal != "" {
		return v.FVal
	}
	if v.TimeVal != "" {
		return v.TimeVal
	}
	return ""
}

type QuotedString struct {
	Str string `@(Quoted_string|Ticked_string) `
}

func (q QuotedString) String() string {
	return q.Str
}

func (q *QuotedString) Unquote() (string, error) {
	str := q.Str
	if q.Str[0] == '`' {
		str = str[1 : len(str)-1]
		str = strings.ReplaceAll(str, "\\`", "`")
		str = strings.ReplaceAll(str, `\`, `\\`)
		str = strings.ReplaceAll(str, `"`, `\"`)
		str = `"` + str + `"`
	}
	var res string = ""
	err := json.Unmarshal([]byte(str), &res)
	return res, err
}
