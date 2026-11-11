package traceql_ir

import (
	"errors"

	traceql_parser "github.com/metrico/qryn/v5/reader/traceql/traceql_parser"
)

// Parse parses and lowers a TraceQL query.
func Parse(query string) (*Query, error) {
	script, err := traceql_parser.Parse(query)
	if err != nil {
		return nil, err
	}
	return Lower(script)
}

// Lower converts a parsed script into its IR.
func Lower(script *traceql_parser.TraceQLScript) (*Query, error) {
	pipeline, err := lowerPipeline(script.Pipeline.Head, script.Pipeline.Stages)
	if err != nil {
		return nil, err
	}
	metrics, err := lowerMetricsFn(script.MetricsFn)
	if err != nil {
		return nil, err
	}
	q := &Query{Pipeline: pipeline, Metrics: metrics, Hints: lowerHints(script.WithHints)}
	if s := script.SecondStage; s != nil {
		q.SecondStage = &SecondStage{Fn: s.Fn, Count: s.Count, Labels: s.Labels, Hints: lowerHints(s.WithHints)}
	}
	return q, nil
}

var (
	errLeadingCoalesce = errors.New("syntax error: unexpected coalesce()")
	errSelectField     = errors.New("syntax error: select() takes attributes and intrinsics only")
)

func lowerPipeline(head traceql_parser.PipelineElement, stages []traceql_parser.PipelineElement) (Pipeline, error) {
	if head.Coalesce != nil {
		return Pipeline{}, errLeadingCoalesce
	}
	element, err := lowerElement(head)
	if err != nil {
		return Pipeline{}, err
	}
	return lowerStages(element, stages)
}

func lowerStages(head Element, stages []traceql_parser.PipelineElement) (Pipeline, error) {
	res := Pipeline{Elements: []Element{head}}
	for _, stage := range stages {
		element, err := lowerElement(stage)
		if err != nil {
			return Pipeline{}, err
		}
		res.Elements = append(res.Elements, element)
	}
	return res, nil
}

func lowerElement(e traceql_parser.PipelineElement) (Element, error) {
	switch {
	case e.ScalarFilter != nil:
		return lowerScalarFilter(e.ScalarFilter)
	case e.GroupBy != nil:
		expr, err := lowerField(&e.GroupBy.Expr)
		if err != nil {
			return nil, err
		}
		return &GroupBy{Expr: expr}, nil
	case e.Coalesce != nil:
		return &Coalesce{}, nil
	case e.Select != nil:
		return lowerSelect(e.Select)
	}
	return lowerSpansetExpr(e.Spanset)
}

// lowerSpansetExpr turns an operand chain into a tree. Structural operators
// bind tighter than `&&` and `||`, which share one level; all are
// left-associative.
func lowerSpansetExpr(expr *traceql_parser.SpansetExpr) (SpansetExpr, error) {
	var (
		res    SpansetExpr
		joinOp SpansetOperator
	)
	term, err := lowerOperand(expr.Head)
	if err != nil {
		return nil, err
	}
	flush := func() {
		if res == nil {
			res = term
		} else {
			res = &SpansetOp{Op: joinOp, LHS: res, RHS: term}
		}
	}
	for _, next := range expr.Ops {
		op := SpansetOperator(next.Op)
		operand, err := lowerOperand(next.Operand)
		if err != nil {
			return nil, err
		}
		if op == OpAnd || op == OpOr {
			flush()
			term, joinOp = operand, op
		} else {
			term = &SpansetOp{Op: op, LHS: term, RHS: operand}
		}
	}
	flush()
	return res, nil
}

// lowerOperand lowers a selector, or a parenthesised pipeline: a Group when
// it is a lone spanset expression, else a Pipeline.
func lowerOperand(o traceql_parser.Operand) (SpansetExpr, error) {
	if o.Paren == nil {
		filter, err := lowerOptionalField(o.Selector.Filter)
		if err != nil {
			return nil, err
		}
		return &Selector{Filter: filter}, nil
	}
	head, err := lowerSpansetExpr(&o.Paren.Head)
	if err != nil {
		return nil, err
	}
	if len(o.Paren.Stages) == 0 {
		return &Group{Expr: head}, nil
	}
	res, err := lowerStages(head, o.Paren.Stages)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func lowerScalarFilter(f *traceql_parser.ScalarFilter) (*ScalarFilter, error) {
	lhs, err := lowerScalarExpr(f.LHS)
	if err != nil {
		return nil, err
	}
	rhs, err := lowerScalarExpr(f.RHS)
	if err != nil {
		return nil, err
	}
	return &ScalarFilter{Op: f.Op, LHS: lhs, RHS: rhs}, nil
}

func lowerScalarExpr(e traceql_parser.ScalarExpr) (ScalarExpr, error) {
	res, err := lowerScalarTerm(e.Head)
	if err != nil {
		return nil, err
	}
	for _, op := range e.Ops {
		rhs, err := lowerScalarTerm(op.Term)
		if err != nil {
			return nil, err
		}
		res = &ScalarOp{Op: op.Op, LHS: res, RHS: rhs}
	}
	return res, nil
}

func lowerScalarTerm(t traceql_parser.ScalarTerm) (ScalarExpr, error) {
	res, err := lowerScalarFactor(t.Head)
	if err != nil {
		return nil, err
	}
	for _, op := range t.Ops {
		rhs, err := lowerScalarFactor(op.Factor)
		if err != nil {
			return nil, err
		}
		res = &ScalarOp{Op: op.Op, LHS: res, RHS: rhs}
	}
	return res, nil
}

func lowerScalarFactor(f traceql_parser.ScalarFactor) (ScalarExpr, error) {
	base, err := lowerScalarAtom(f.Base)
	if err != nil || f.Exp == nil {
		return base, err
	}
	exp, err := lowerScalarFactor(*f.Exp)
	if err != nil {
		return nil, err
	}
	return &ScalarOp{Op: "^", LHS: base, RHS: exp}, nil
}

func lowerScalarAtom(a traceql_parser.ScalarAtom) (ScalarExpr, error) {
	switch {
	case a.Aggregate != nil:
		return lowerAggregate(a.Aggregate)
	case a.Literal != nil:
		if a.Literal.Unit != "" {
			return &Static{Kind: ValueDuration, Text: a.Literal.Num + a.Literal.Unit}, nil
		}
		return &Static{Kind: ValueNumber, Text: a.Literal.Num}, nil
	}
	return lowerScalarExpr(*a.Paren)
}

// lowerAggregate lowers an aggregate. A lone unquoted word that names no
// field is read as an attribute of that name.
func lowerAggregate(a *traceql_parser.Aggregate) (*Aggregate, error) {
	arg, err := lowerOptionalField(a.Arg)
	if err != nil {
		return nil, err
	}
	if s, ok := arg.(*Static); ok && s.Kind == ValueIdent {
		arg = &Attribute{Name: s.Text}
	}
	return &Aggregate{Fn: a.Fn, Arg: arg}, nil
}

func lowerMetricsFn(m *traceql_parser.MetricsPipelineStage) (*MetricsFn, error) {
	if m == nil {
		return nil, nil
	}
	res := &MetricsFn{Fn: m.Fn, Attr: m.Attr, Percentile: m.Percentile}
	if m.By != nil {
		res.By = m.By.Labels
	}
	if c := m.CompSel; c != nil {
		filter, err := lowerOptionalField(c.Filter)
		if err != nil {
			return nil, err
		}
		res.Compare = &CompareArgs{
			Filter:       filter,
			Count:        c.Count,
			BaselineFrom: c.BaselineFrom,
			BaselineTo:   c.BaselineTo,
		}
	}
	return res, nil
}

func lowerHints(w *traceql_parser.WithClause) []Hint {
	if w == nil {
		return nil
	}
	res := make([]Hint, len(w.Hints))
	for i, h := range w.Hints {
		res[i] = Hint{Key: h.Key, Value: h.Value}
	}
	return res
}

// lowerSelect lowers select(); each argument is a lone attribute or intrinsic.
func lowerSelect(s *traceql_parser.Select) (*Select, error) {
	res := &Select{}
	for _, f := range s.Fields {
		atom := f.Head.Atom
		if len(f.Ops) > 0 || len(f.Head.Unary) > 0 || atom.Name == "" || !isFieldName(atom.Name) {
			return nil, errSelectField
		}
		res.Fields = append(res.Fields, &Attribute{Name: atom.Name})
	}
	return res, nil
}
