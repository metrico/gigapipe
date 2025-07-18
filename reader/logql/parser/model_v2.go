package parser

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type LogQLScript struct {
	StrSelector      *StrSelector      `@@`
	LRAOrUnwrap      *LRAOrUnwrap      `| @@`
	AggOperator      *AggOperator      `| @@`
	Macros           *MacrosOp         `| @@`
	TopK             *TopK             `| @@`
	QuantileOverTime *QuantileOverTime `| @@`
}

func (l LogQLScript) String() string {
	if l.StrSelector != nil {
		return l.StrSelector.String()
	}
	if l.LRAOrUnwrap != nil {
		return l.LRAOrUnwrap.String()
	}
	if l.AggOperator != nil {
		return l.AggOperator.String()
	}
	if l.Macros != nil {
		return l.Macros.String()
	}
	if l.TopK != nil {
		return l.TopK.String()
	}
	if l.QuantileOverTime != nil {
		return l.QuantileOverTime.String()
	}
	return ""
}

type StrSelector struct {
	StrSelCmds []StrSelCmd           `"{" @@ ("," @@ )* "}" `
	Pipelines  []StrSelectorPipeline `@@*`
}

func (l StrSelector) String() string {
	sel := make([]string, len(l.StrSelCmds))
	for i, c := range l.StrSelCmds {
		sel[i] = c.Label.String() + c.Op + c.Val.String()
	}
	ppl := make([]string, len(l.Pipelines))
	for i, p := range l.Pipelines {
		ppl[i] = p.String()
	}
	return fmt.Sprintf("{%s}%s",
		strings.Join(sel, ","),
		strings.Join(ppl, " "))
}

type StrSelCmd struct {
	Label LabelName    `@@`
	Op    string       `@("="|"!="|"=~"|"!~")`
	Val   QuotedString `@@`
}

type LabelName struct {
	Name string `@(Macros_function|Label_name)`
}

func (l LabelName) String() string {
	return l.Name
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

type StrSelectorPipeline struct {
	LineFilter  *LineFilter  `@@ `
	LabelFilter *LabelFilter `| "|" @@ `
	Parser      *Parser      `| "|" @@ `
	LineFormat  *LineFormat  `| "|" @@ `
	LabelFormat *LabelFormat `| "|" @@ `
	Unwrap      *Unwrap      `| "|" @@ `
	Drop        *Drop        `| "|" @@ `
}

func (s *StrSelectorPipeline) String() string {
	if s.LineFilter != nil {
		return s.LineFilter.String()
	}

	if s.LabelFilter != nil {
		return "| " + s.LabelFilter.String()
	}

	if s.Parser != nil {
		return s.Parser.String()
	}

	if s.LineFormat != nil {
		return s.LineFormat.String()
	}

	if s.LabelFormat != nil {
		return s.LabelFormat.String()
	}

	if s.Unwrap != nil {
		return s.Unwrap.String()
	}

	return s.Drop.String()
}

type LineFilter struct {
	Fn  string       `@("|="|"!="|"|~"|"!~"|"|>")`
	Val QuotedString `@@`
}

func (l *LineFilter) String() string {
	return fmt.Sprintf(" %s %s", l.Fn, l.Val.String())
}

type LabelFilter struct {
	Head Head         `@@`
	Op   string       `(@("and"|"or"))?`
	Tail *LabelFilter `@@?`
}

func (l *LabelFilter) String() string {
	bld := strings.Builder{}
	bld.WriteString(l.Head.String())
	if l.Op == "" {
		return bld.String()
	}
	bld.WriteString(" ")
	bld.WriteString(l.Op)
	bld.WriteString(" ")
	bld.WriteString(l.Tail.String())
	return bld.String()
}

type Head struct {
	ComplexHead *LabelFilter       `"(" @@ ")"`
	SimpleHead  *SimpleLabelFilter `|@@`
}

func (h *Head) String() string {
	if h.ComplexHead != nil {
		return "(" + h.ComplexHead.String() + ")"
	}
	return h.SimpleHead.String()
}

type SimpleLabelFilter struct {
	Label  LabelName     `@@`
	Fn     string        `@("="|"!="|"!~"|"=="|">="|">"|"<="|"<"|"=~")`
	StrVal *QuotedString `(@@`
	NumVal string        `| @(Integer "."? Integer*))`
}

func (s *SimpleLabelFilter) String() string {
	bld := strings.Builder{}
	bld.WriteString(fmt.Sprintf("%s %s ", s.Label, s.Fn))
	if s.StrVal != nil {
		bld.WriteString(s.StrVal.String())
	} else {
		bld.WriteString(s.NumVal)
	}
	return bld.String()

}

type Parser struct {
	Fn           string        `@("json"|"logfmt"|"regexp")`
	ParserParams []ParserParam `@@? ("," @@)*`
}

func (p *Parser) String() string {
	if p.ParserParams == nil {
		return fmt.Sprintf("| %s", p.Fn)
	}
	params := make([]string, len(p.ParserParams))
	for i, param := range p.ParserParams {
		params[i] = param.String()
	}
	return fmt.Sprintf("| %s %s", p.Fn, strings.Join(params, ", "))
}

type ParserParam struct {
	Label *LabelName   `(@@ "=" )?`
	Val   QuotedString `@@`
}

func (p *ParserParam) String() string {
	if p.Label == nil {
		return p.Val.String()
	}
	return fmt.Sprintf("%s = %s", p.Label, p.Val.String())
}

type LineFormat struct {
	Val QuotedString `"line_format" @@ `
}

func (f *LineFormat) String() string {
	return fmt.Sprintf("| line_format %s", f.Val.String())
}

type LabelFormat struct {
	LabelFormatOps []LabelFormatOp `"label_format" @@ ("," @@ )*`
}

func (l *LabelFormat) String() string {
	ops := make([]string, len(l.LabelFormatOps))
	for i, op := range l.LabelFormatOps {
		ops[i] = op.String()
	}
	return fmt.Sprintf("| label_format %s", strings.Join(ops, ", "))
}

type LabelFormatOp struct {
	Label    LabelName     `@@ "=" `
	LabelVal *LabelName    `(@@`
	ConstVal *QuotedString `|@@)`
}

func (l *LabelFormatOp) String() string {
	bld := strings.Builder{}
	bld.WriteString(l.Label.String())
	bld.WriteString(" = ")
	if l.LabelVal != nil {
		bld.WriteString(l.LabelVal.String())
	} else {
		bld.WriteString(l.ConstVal.String())
	}
	return bld.String()
}

type Unwrap struct {
	Fn    string    `@("unwrap"|"unwrap_value")`
	Label LabelName ` @@?`
}

func (u *Unwrap) String() string {
	return fmt.Sprintf("| %s %s", u.Fn, u.Label.String())
}

type Drop struct {
	Fn     string      `@("drop")`
	Params []DropParam `@@? ("," @@)*`
}

func (d *Drop) String() string {
	params := make([]string, len(d.Params))
	for i, param := range d.Params {
		params[i] = param.String()
	}
	return fmt.Sprintf("| %s %s", d.Fn, strings.Join(params, ","))
}

type DropParam struct {
	Label LabelName     `@@`
	Val   *QuotedString `("=" @@)?`
}

func (d *DropParam) String() string {
	bld := strings.Builder{}
	bld.WriteString(d.Label.String())
	if d.Val != nil {
		bld.WriteString("=")
		bld.WriteString(d.Val.String())
	}
	return bld.String()
}

type LRAOrUnwrap struct {
	Fn string `@("rate"|"count_over_time"|"bytes_rate"|"bytes_over_time"|"absent_over_time"|
"sum_over_time"|"avg_over_time"|"max_over_time"|"min_over_time"|"first_over_time"|"last_over_time"|
"stdvar_over_time"|"stddev_over_time")`
	ByOrWithoutPrefix *ByOrWithout `( @@)?`
	StrSel            StrSelector  `"(" @@ `
	Time              string       `"[" @Integer `
	TimeUnit          string       `@("ns"|"us"|"ms"|"s"|"m"|"h") "]" `
	Offset            *Offset      `("offset" @@)? ")" `
	ByOrWithoutSuffix *ByOrWithout `@@?`
	Comparison        *Comparison  `@@?`
}

func (l LRAOrUnwrap) String() string {
	res := l.Fn
	if l.ByOrWithoutPrefix != nil {
		res += " " + l.ByOrWithoutPrefix.String()
	}
	res += " (" + l.StrSel.String() + "[" + l.Time + l.TimeUnit + "]"
	if l.Offset != nil {
		res += " " + l.Offset.String()
	}
	res += ")"
	if l.ByOrWithoutPrefix == nil && l.ByOrWithoutSuffix != nil {
		res += l.ByOrWithoutSuffix.String()
	}
	if l.Comparison != nil {
		res += l.Comparison.String()
	}
	return res
}

type Comparison struct {
	Fn  string `@("=="|"!="|">"|">="|"<"|"<=") `
	Val string `@(Integer "."? Integer*)`
}

func (l Comparison) String() string {
	return l.Fn + " " + l.Val
}

type ByOrWithout struct {
	Fn     string      `@("by"|"without") `
	Labels []LabelName `"(" @@ ("," @@)* ")" `
}

func (l ByOrWithout) String() string {
	labels := make([]string, len(l.Labels))
	for i, label := range l.Labels {
		labels[i] = label.String()
	}
	return fmt.Sprintf("%s (%s)", l.Fn, strings.Join(labels, ","))
}

func (l ByOrWithout) LabelNames() []string {
	labels := make([]string, len(l.Labels))
	for i, label := range l.Labels {
		labels[i] = label.String()
	}
	return labels
}

type AggOperator struct {
	Fn                string       `@("sum"|"min"|"max"|"avg"|"stddev"|"stdvar"|"count") `
	ByOrWithoutPrefix *ByOrWithout `@@?`
	LRAOrUnwrap       LRAOrUnwrap  `"(" @@ ")" `
	ByOrWithoutSuffix *ByOrWithout `@@?`
	Comparison        *Comparison  `@@?`
}

func (l AggOperator) String() string {
	res := l.Fn
	if l.ByOrWithoutPrefix != nil {
		res += " " + l.ByOrWithoutPrefix.String()
	}

	res += " (" + l.LRAOrUnwrap.String() + ")"
	if l.ByOrWithoutPrefix == nil && l.ByOrWithoutSuffix != nil {
		res += l.ByOrWithoutSuffix.String()
	}
	if l.Comparison != nil {
		res += l.Comparison.String()
	}
	return res
}

type MacrosOp struct {
	Name   string         `@Macros_function`
	Params []QuotedString `"(" @@? ("," @@)* ")"`
}

func (l MacrosOp) String() string {
	params := make([]string, len(l.Params))
	for i, p := range l.Params {
		params[i] = p.String()
	}
	return fmt.Sprintf("%s(%s)", l.Name, strings.Join(params, ","))
}

type TopK struct {
	Fn               string            `@("topk"|"bottomk")`
	Param            string            `"(" @(Integer+ "."? Integer*) "," `
	LRAOrUnwrap      *LRAOrUnwrap      `(@@`
	AggOperator      *AggOperator      `| @@`
	QuantileOverTime *QuantileOverTime `| @@)")"`
	Comparison       *Comparison       `@@?`
}

func (l TopK) String() string {
	fn := ""
	cmp := ""
	if l.LRAOrUnwrap != nil {
		fn = l.LRAOrUnwrap.String()
	}
	if l.AggOperator != nil {
		fn = l.AggOperator.String()
	}
	if l.Comparison != nil {
		cmp = l.Comparison.String()
	}
	return fmt.Sprintf("%s(%s, %s)%s", l.Fn, l.Param, fn, cmp)
}

type QuantileOverTime struct {
	Fn                string       `@"quantile_over_time" `
	ByOrWithoutPrefix *ByOrWithout `@@?`
	Param             string       `"(" @(Integer+ "."? Integer*) "," `
	StrSel            StrSelector  `@@`
	Time              string       `"[" @Integer `
	TimeUnit          string       `@("ns"|"us"|"ms"|"s"|"m"|"h") "]" ")" `
	ByOrWithoutSuffix *ByOrWithout `@@?`
	Comparison        *Comparison  `@@?`
}

func (l QuantileOverTime) String() string {
	res := l.Fn
	if l.ByOrWithoutPrefix != nil {
		res += " " + l.ByOrWithoutPrefix.String()
	}
	res += " (" + l.Param + ", " + l.StrSel.String() + "[" + l.Time + l.TimeUnit + "])"
	if l.ByOrWithoutPrefix == nil && l.ByOrWithoutSuffix != nil {
		res += l.ByOrWithoutSuffix.String()
	}
	if l.Comparison != nil {
		res += l.Comparison.String()
	}
	return res
}

type Offset struct {
	Time     string `@Integer `
	TimeUnit string `@("ns"|"us"|"ms"|"s"|"m"|"h")`
}

func (l Offset) String() string {
	return fmt.Sprintf("offset %s%s", l.Time, l.TimeUnit)
}

func (l Offset) Duration() (time.Duration, error) {
	dur, err := time.ParseDuration(fmt.Sprintf("%s%s", l.Time, l.TimeUnit))
	if err != nil {
		return 0, err
	}
	return dur, nil
}
