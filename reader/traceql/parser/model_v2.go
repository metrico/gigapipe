package traceql_parser

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
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
	Pipeline     []Pipeline       `@@*`
}

func (s Selector) String() string {
	res := "{" + s.AttrSelector.String() + "}"
	for _, pipeline := range s.Pipeline {
		res += pipeline.String()
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

type Pipeline struct {
	Agg      *Aggregator     `"|" (@@`
	Selector *ResultSelector `| @@`
	By       *GroupBy        `| @@)`
}

func (p Pipeline) String() string {
	res := ""
	if p.Agg != nil {
		res += " | " + p.Agg.String()
	} else if p.Selector != nil {
		res += " | " + p.Selector.String()
	}
	return res
}

type Aggregator struct {
	Fn          string    `@("count"|"sum"|"min"|"max"|"avg")`
	Attr        LabelName `"(" @@? ")"`
	Cmp         string    `@("="|"!="|"<"|"<="|">"|">=")`
	Num         string    `@Minus? @Integer @Dot? @Integer?`
	Measurement string    `@("ns"|"us"|"ms"|"s"|"m"|"h"|"d")?`
}

func (a Aggregator) String() string {
	return a.Fn + "(" + a.Attr.String() + ") " + a.Cmp + " " + a.Num + a.Measurement
}

type AttrSelector struct {
	Label LabelName `@@`
	Op    string    `@("="|"!="|"<"|"<="|">"|">="|"=~"|"!~")`
	Val   Value     `@@`
}

func (a AttrSelector) String() string {
	return a.Label.String() + " " + a.Op + " " + a.Val.String()
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

type ResultSelector struct {
	Attributes []LabelName `"select" "(" @@ ( "," @@ )* ")"`
}

func (r ResultSelector) String() string {
	var attributes []string
	for _, attr := range r.Attributes {
		attributes = append(attributes, attr.String())
	}
	return fmt.Sprintf("select(%s)", strings.Join(attributes, ", "))
}

type GroupBy struct {
	Attributes []LabelName `"by" "(" @@ ( "," @@ )* ")"`
}

func (g GroupBy) String() string {
	labels := make([]string, len(g.Attributes))
	for i, label := range g.Attributes {
		labels[i] = label.String()
	}
	return fmt.Sprintf("by(%s)", strings.Join(labels, ", "))
}

type LabelName struct {
	Parts []string `(@Label_name | @Dot @Quoted_string)+`
}

func (l LabelName) Path() []string {
	var path []string
	for _, part := range l.Parts {
		part = strings.Trim(part, `.`)
		if part == "" {
			continue
		}
		if part[0] == '"' {
			part, _ = strconv.Unquote(part)
		}
		path = append(path, part)
	}
	return path
}

func (l LabelName) String() string {
	return strings.Join(l.Parts, "")
}

func Visit(node any, f func(node any) error) error {
	if node == nil {
		return nil
	}

	nodePtr := reflect.ValueOf(node)
	if nodePtr.Kind() != reflect.Ptr {
		nodePtr = reflect.New(reflect.TypeOf(node))
		nodePtr.Elem().Set(reflect.ValueOf(node))
	}

	if nodePtr.IsNil() {
		return nil
	}

	var children []any
	switch v := nodePtr.Interface().(type) {
	case *TraceQLScript:
		children = append(children, &v.Head, v.Tail)
	case *Selector:
		children = append(children, v.AttrSelector)
		for _, pipeline := range v.Pipeline {
			children = append(children, &pipeline)
		}
	case *AttrSelectorExp:
		children = append(children, v.Head, v.ComplexHead, v.Tail)
	case *Pipeline:
		children = append(children, v.Agg, v.Selector)
	case *AttrSelector:
		children = append(children, &v.Val)
	case *Value:
		children = append(children, &v.StrVal)
	case *ResultSelector, *QuotedString, *Aggregator:
		break
	default:
		return nil
	}
	err := f(nodePtr.Interface())
	if err != nil {
		return err
	}
	for _, child := range children {
		err = Visit(child, f)
		if err != nil {
			return err
		}
	}
	return nil
}
