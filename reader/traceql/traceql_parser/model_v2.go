package traceql_parser

import (
	"encoding/json"
	"fmt"
	"strings"
)

// TraceQLScript is a pipeline followed by the optional metrics function,
// hints and second stage.
type TraceQLScript struct {
	Pipeline    Pipeline              `@@`
	MetricsFn   *MetricsPipelineStage `@@?`
	WithHints   *WithClause           `@@?`
	SecondStage *SecondPipelineStage  `@@?`
}

func (l TraceQLScript) String() string {
	res := l.Pipeline.String()
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

// Pipeline is `element ('|' element)*`.
type Pipeline struct {
	Head   PipelineElement   `@@`
	Stages []PipelineElement `( "|" @@ )*`
}

func (p Pipeline) String() string {
	return pipelineString(p.Head.String(), p.Stages)
}

func pipelineString(head string, stages []PipelineElement) string {
	res := head
	for _, s := range stages {
		res += " | " + s.String()
	}
	return res
}

// PipelineElement is a spanset expression, a scalar filter, `by()`,
// `coalesce()` or `select()`.
type PipelineElement struct {
	Spanset      *SpansetExpr  `  @@`
	ScalarFilter *ScalarFilter `| @@`
	GroupBy      *GroupBy      `| @@`
	Coalesce     *Coalesce     `| @@`
	Select       *Select       `| @@`
}

func (e PipelineElement) String() string {
	switch {
	case e.ScalarFilter != nil:
		return e.ScalarFilter.String()
	case e.GroupBy != nil:
		return e.GroupBy.String()
	case e.Coalesce != nil:
		return e.Coalesce.String()
	case e.Select != nil:
		return e.Select.String()
	}
	return e.Spanset.String()
}

// Select is `select(fieldexpr, ...)`.
type Select struct {
	Fields []FieldExpr `"select" "(" @@ ( "," @@ )* ")"`
}

func (s Select) String() string {
	fields := make([]string, len(s.Fields))
	for i, f := range s.Fields {
		fields[i] = f.String()
	}
	return "select(" + strings.Join(fields, ", ") + ")"
}

// GroupBy is `by(fieldexpr)`.
type GroupBy struct {
	Expr FieldExpr `"by" "(" @@ ")"`
}

func (g GroupBy) String() string {
	return "by(" + g.Expr.String() + ")"
}

// Coalesce is `coalesce()`.
type Coalesce struct {
	Fn string `@"coalesce" "(" ")"`
}

func (Coalesce) String() string {
	return "coalesce()"
}

// SpansetExpr is operands joined by spanset operators, in written order.
type SpansetExpr struct {
	Head Operand            `@@`
	Ops  []SpansetOperation `@@*`
}

func (s SpansetExpr) String() string {
	res := s.Head.String()
	for _, op := range s.Ops {
		res += " " + op.Op + " " + op.Operand.String()
	}
	return res
}

// SpansetOperation is a spanset operator and the operand after it. Gt, Lt and
// Nre are structural between spansets.
type SpansetOperation struct {
	Op      string  `@(Gt|Lt|Sibling|NotChild|NotParent|Nre|UnionChild|UnionParent|UnionSibling|Descendant|Ancestor|NotDescendant|NotAncestor|UnionDescendant|UnionAncestor|And|Or)`
	Operand Operand `@@`
}

// Operand is a selector or a parenthesised pipeline.
type Operand struct {
	Paren    *ParenPipeline `  "(" @@ ")"`
	Selector Selector       `| @@`
}

func (o Operand) String() string {
	if o.Paren != nil {
		return "(" + o.Paren.String() + ")"
	}
	return o.Selector.String()
}

// ParenPipeline is a parenthesised pipeline; it starts with a spanset expression.
type ParenPipeline struct {
	Head   SpansetExpr       `@@`
	Stages []PipelineElement `( "|" @@ )*`
}

func (p ParenPipeline) String() string {
	return pipelineString(p.Head.String(), p.Stages)
}

type Selector struct {
	Filter *FieldExpr `"{" @@? "}"`
}

func (s Selector) String() string {
	if s.Filter == nil {
		return "{}"
	}
	return "{" + s.Filter.String() + "}"
}

// ScalarFilter is `LHS Op RHS` over scalar expressions.
type ScalarFilter struct {
	LHS ScalarExpr `@@`
	Op  string     `@("="|"!="|"<"|"<="|">"|">=")`
	RHS ScalarExpr `@@`
}

func (f ScalarFilter) String() string {
	return f.LHS.String() + " " + f.Op + " " + f.RHS.String()
}

// ScalarExpr is `term (('+'|'-') term)*`.
type ScalarExpr struct {
	Head ScalarTerm    `@@`
	Ops  []ScalarSumOp `@@*`
}

func (e ScalarExpr) String() string {
	res := e.Head.String()
	for _, op := range e.Ops {
		res += " " + op.Op + " " + op.Term.String()
	}
	return res
}

type ScalarSumOp struct {
	Op   string     `@(Plus|Minus)`
	Term ScalarTerm `@@`
}

// ScalarTerm is `factor (('*'|'/'|'%') factor)*`.
type ScalarTerm struct {
	Head ScalarFactor  `@@`
	Ops  []ScalarMulOp `@@*`
}

func (t ScalarTerm) String() string {
	res := t.Head.String()
	for _, op := range t.Ops {
		res += " " + op.Op + " " + op.Factor.String()
	}
	return res
}

type ScalarMulOp struct {
	Op     string       `@(Mul|Div|Mod)`
	Factor ScalarFactor `@@`
}

// ScalarFactor is `atom ('^' factor)?`, right-associative.
type ScalarFactor struct {
	Base ScalarAtom    `@@`
	Exp  *ScalarFactor `( Pow @@ )?`
}

func (f ScalarFactor) String() string {
	if f.Exp != nil {
		return f.Base.String() + " ^ " + f.Exp.String()
	}
	return f.Base.String()
}

// ScalarAtom is an aggregate, a number or duration literal, or a
// parenthesised scalar expression.
type ScalarAtom struct {
	Aggregate *Aggregate     `  @@`
	Literal   *ScalarLiteral `| @@`
	Paren     *ScalarExpr    `| "(" @@ ")"`
}

func (a ScalarAtom) String() string {
	switch {
	case a.Aggregate != nil:
		return a.Aggregate.String()
	case a.Literal != nil:
		return a.Literal.String()
	}
	return "(" + a.Paren.String() + ")"
}

// Aggregate is `count()` or `avg|min|max|sum(fieldexpr)`.
type Aggregate struct {
	Fn  string     `( @"count" "(" ")" | @("avg"|"min"|"max"|"sum") "("`
	Arg *FieldExpr `  @@ ")" )`
}

func (a Aggregate) String() string {
	if a.Arg == nil {
		return a.Fn + "()"
	}
	return a.Fn + "(" + a.Arg.String() + ")"
}

// ScalarLiteral is a number, with its duration unit if any.
type ScalarLiteral struct {
	Num  string `@Minus? (@Float | @Integer @Dot? @Integer?)`
	Unit string `@("ns"|"us"|"ms"|"s"|"m"|"h"|"d")?`
}

func (l ScalarLiteral) String() string {
	return l.Num + l.Unit
}

// FieldExpr is a field expression as written: operands joined by binary
// operators, which lowering groups by precedence.
type FieldExpr struct {
	Head FieldOperand  `@@`
	Ops  []FieldBinary `@@*`
}

func (e FieldExpr) String() string {
	res := e.Head.String()
	for _, op := range e.Ops {
		res += " " + op.Op + " " + op.Operand.String()
	}
	return res
}

// FieldBinary is a binary operator and the operand after it.
type FieldBinary struct {
	Op      string       `@("+"|"-"|"*"|"/"|"%"|"^"|"="|"!="|"<"|"<="|">"|">="|"=~"|"!~"|"&&"|"||")`
	Operand FieldOperand `@@`
}

// FieldOperand is an atom after its unary operators, outermost first.
type FieldOperand struct {
	Unary []string  `@("-"|"!")*`
	Atom  FieldAtom `@@`
}

func (o FieldOperand) String() string {
	return strings.Join(o.Unary, "") + o.Atom.String()
}

// FieldAtom is a parenthesised field expression, a literal or a name: an
// attribute, an intrinsic or a keyword.
type FieldAtom struct {
	Paren  *FieldExpr     `  "(" @@ ")"`
	Str    *QuotedString  `| @@`
	Number *NumberLiteral `| @@`
	Name   string         `| @Label_name`
}

func (a FieldAtom) String() string {
	switch {
	case a.Paren != nil:
		return "(" + a.Paren.String() + ")"
	case a.Str != nil:
		return a.Str.Str
	case a.Number != nil:
		return a.Number.String()
	}
	return a.Name
}

// NumberLiteral is an unsigned number, with its duration unit if any.
type NumberLiteral struct {
	Num  string `(@Float | @Integer @Dot? @Integer?)`
	Unit string `@("ns"|"us"|"ms"|"s"|"m"|"h"|"d")?`
}

func (l NumberLiteral) String() string {
	return l.Num + l.Unit
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
	Filter       *FieldExpr `"{" @@? "}"`
	Comma        *struct{}  `","?`
	Count        int        `@Integer?`
	BaselineFrom *int64     `("," @Integer)?`
	BaselineTo   *int64     `("," @Integer)?`
}

func (c CompareSelector) String() string {
	sel := ""
	if c.Filter != nil {
		sel = c.Filter.String()
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
