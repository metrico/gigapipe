package traceql_parser

import (
	"encoding/json"
	"fmt"
	"strings"
)

type TraceQLScript struct {
	ParenExpr *TraceQLScript `( "(" @@ ")"`
	Head      Selector       `| @@ )`
	Op        string         `@(Descendant|NotDescendant|Ancestor|NotAncestor|Sibling|And|Or)?`
	Tail      *TraceQLScript `@@?`
	// MetricsFn at the script level supports: ({A} &>> {B}) | rate()
	MetricsFn   *MetricsPipelineStage `@@?`
	WithHints   *WithClause           `@@?`
	SecondStage *SecondPipelineStage  `@@?`
}

func (l TraceQLScript) String() string {
	var res string
	if l.ParenExpr != nil {
		res = "(" + l.ParenExpr.String() + ")"
	} else {
		res = l.Head.String()
	}
	if l.Op != "" && l.Tail != nil {
		res += " " + l.Op + " " + l.Tail.String()
	}
	if l.MetricsFn != nil {
		res += " " + l.MetricsFn.String()
	}
	if l.WithHints != nil {
		res += " " + l.WithHints.String()
	}
	if l.SecondStage != nil {
		res += " " + l.SecondStage.String()
	}
	return res
}

// ResolvedHead returns the effective head selector, following ParenExpr if needed.
func (l TraceQLScript) ResolvedHead() *Selector {
	if l.ParenExpr != nil {
		return l.ParenExpr.ResolvedHead()
	}
	return &l.Head
}

// ResolvedMetricsFn returns the MetricsFn from either the script level or the head selector.
func (l TraceQLScript) ResolvedMetricsFn() *MetricsPipelineStage {
	if l.MetricsFn != nil {
		return l.MetricsFn
	}
	head := l.ResolvedHead()
	if head != nil {
		return head.MetricsFn
	}
	return nil
}

type Selector struct {
	AttrSelector *AttrSelectorExp      `"{" @@? "}"`
	Aggregator   *Aggregator           `@@?`
	MetricsFn    *MetricsPipelineStage `@@?`
}

func (s Selector) String() string {
	res := "{" + s.AttrSelector.String() + "}"
	if s.Aggregator != nil {
		res += " " + s.Aggregator.String()
	}
	if s.MetricsFn != nil {
		res += " " + s.MetricsFn.String()
	}
	return res
}

type AttrSelectorExp struct {
	Head        *AttrSelector    `(@@`
	BoolLiteral string           `| @("true"|"false")`
	ComplexHead *AttrSelectorExp `| "(" @@ ")" )`
	AndOr       string           `@(And|Or)?`
	Tail        *AttrSelectorExp `@@?`
}

func (a AttrSelectorExp) String() string {
	res := ""
	if a.Head != nil {
		res += a.Head.String()
	}
	if a.BoolLiteral != "" {
		res += a.BoolLiteral
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
	Num         string `@Minus? (@Float | @Integer @Dot? @Integer?)`
	Measurement string `@("ns"|"us"|"ms"|"s"|"m"|"h"|"d")?`
}

func (a Aggregator) String() string {
	return "| " + a.Fn + "(" + a.Attr + ") " + a.Cmp + " " + a.Num + a.Measurement
}

type AttrSelector struct {
	Label string `@Label_name`
	Op    string `@("="|"!="|"<"|"<="|">"|">="|"=~"|"!~")?`
	Val   Value  `@@?`
}

func (a AttrSelector) String() string {
	return a.Label + " " + a.Op + " " + a.Val.String()
}

type Value struct {
	NilVal    string        `@"nil"`
	BoolVal   string        `| @("true"|"false")`
	TimeVal   string        `| (@Float | @Integer @Dot? @Integer?) @("ns"|"us"|"ms"|"s"|"m"|"h"|"d")`
	FVal      string        `| @Minus? (@Float | @Integer @Dot? @Integer?)`
	StrVal    *QuotedString `| @@`
	UnquotVal string        `| @Label_name`
}

func (v Value) String() string {
	if v.NilVal != "" {
		return "nil"
	}
	if v.BoolVal != "" {
		return v.BoolVal
	}
	if v.StrVal != nil {
		return v.StrVal.Str
	}
	if v.FVal != "" {
		return v.FVal
	}
	if v.TimeVal != "" {
		return v.TimeVal
	}
	if v.UnquotVal != "" {
		return v.UnquotVal
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

// MetricsPipelineStage represents: | rate(), | count_over_time(), | compare(), etc.
type MetricsPipelineStage struct {
	Fn         string           `"|" @Label_name`
	Attr       string           `"(" @Label_name?`
	Percentile *float64         `( "," @Float )?`
	CompSel    *CompareSelector `@@?`
	Cb         string           `")"`
	By         *ByClause        `@@?`
}

func (m MetricsPipelineStage) String() string {
	res := "| " + m.Fn + "(" + m.Attr
	if m.Percentile != nil {
		res += fmt.Sprintf(", %g", *m.Percentile)
	}
	if m.CompSel != nil {
		res += m.CompSel.String()
	}
	res += ")"
	if m.By != nil {
		res += " " + m.By.String()
	}
	return res
}

// CompareSelector handles the arguments to compare():
//
//	compare({selector}, N)
//	compare({selector}, N, baselineStartNs, baselineEndNs)
type CompareSelector struct {
	AttrSelector *AttrSelectorExp `"{" @@? "}"`
	Comma        *struct{}        `","?`
	Count        int              `@Integer?`
	BaselineFrom *int64           `("," @Integer)?`
	BaselineTo   *int64           `("," @Integer)?`
}

func (c CompareSelector) String() string {
	sel := ""
	if c.AttrSelector != nil {
		sel = c.AttrSelector.String()
	}
	s := fmt.Sprintf("{%s}, %d", sel, c.Count)
	if c.BaselineFrom != nil {
		s += fmt.Sprintf(", %d", *c.BaselineFrom)
	}
	if c.BaselineTo != nil {
		s += fmt.Sprintf(", %d", *c.BaselineTo)
	}
	return s
}

// ByClause represents: by (label1, label2, ...)
type ByClause struct {
	Labels []string `"by" "(" @Label_name ( "," @Label_name )* ")"`
}

func (b ByClause) String() string {
	return "by (" + strings.Join(b.Labels, ", ") + ")"
}

// WithClause represents: with(key=value, ...)
type WithClause struct {
	Hints []WithHint `"with" "(" @@ ( "," @@ )* ")"`
}

func (w WithClause) String() string {
	parts := make([]string, len(w.Hints))
	for i, h := range w.Hints {
		parts[i] = h.String()
	}
	return "with(" + strings.Join(parts, ", ") + ")"
}

// WithHint represents a single key=value hint like sample=true or sample=0.1
type WithHint struct {
	Key   string `@Label_name "="`
	Value string `@("true"|"false"|Float|Integer|Label_name)`
}

func (h WithHint) String() string {
	return h.Key + "=" + h.Value
}

// SecondPipelineStage represents: | topk(N), | bottomk(N), or | select(attr1, attr2, ...)
type SecondPipelineStage struct {
	Fn        string      `"|" @("topk"|"bottomk"|"select")`
	Count     int         `"(" ( @Integer`
	Labels    []string    `| @Label_name ( "," @Label_name )* ) ")"`
	WithHints *WithClause `@@?`
}

func (s SecondPipelineStage) String() string {
	if s.Fn == "select" {
		return fmt.Sprintf("| select(%s)", strings.Join(s.Labels, ", "))
	}
	return fmt.Sprintf("| %s(%d)", s.Fn, s.Count)
}
