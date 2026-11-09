package traceql_ir

import "strconv"

// hint returns the value of the first hint named key that parse accepts,
// from the hints after the metrics function, then those after the second stage.
func hint[T any](q *Query, key string, parse func(string) (T, bool)) (T, bool) {
	hints := q.Hints
	if q.SecondStage != nil {
		hints = append(append([]Hint{}, hints...), q.SecondStage.Hints...)
	}
	for _, h := range hints {
		if h.Key != key {
			continue
		}
		if v, ok := parse(h.Value); ok {
			return v, true
		}
	}
	var zero T
	return zero, false
}

// SampleFraction is the `sample` hint when it is a number between 0 and 1,
// exclusive.
func (q *Query) SampleFraction() (float64, bool) {
	f, ok := hint(q, "sample", func(v string) (float64, bool) {
		f, err := strconv.ParseFloat(v, 64)
		return f, err == nil
	})
	if !ok || !(f > 0 && f < 1) {
		return 0, false
	}
	return f, true
}

// ExemplarLimit is the `exemplars` hint: the cap on exemplars per series, 0
// for `false` or a cap below one.
func (q *Query) ExemplarLimit() (int, bool) {
	return hint(q, "exemplars", func(v string) (int, bool) {
		if v == "false" {
			return 0, true
		}
		n, err := strconv.Atoi(v)
		return max(n, 0), err == nil
	})
}

// NeedsWholeTrace reports whether evaluating q relates spans across a trace:
// through a spanset operator, a scalar filter or a by() stage.
func (q *Query) NeedsWholeTrace() bool {
	return q.Pipeline.needsWholeTrace()
}

func (p *Pipeline) needsWholeTrace() bool {
	for _, element := range p.Elements {
		switch e := element.(type) {
		case *ScalarFilter, *GroupBy:
			return true
		case SpansetExpr:
			if spansetNeedsWholeTrace(e) {
				return true
			}
		}
	}
	return false
}

func spansetNeedsWholeTrace(expr SpansetExpr) bool {
	switch e := expr.(type) {
	case *Group:
		return spansetNeedsWholeTrace(e.Expr)
	case *Pipeline:
		return e.needsWholeTrace()
	case *SpansetOp:
		return true
	}
	return false
}

// SelectedFields lists the fields of every select() stage of q, nested
// pipelines included, each once in first-seen order.
func (q *Query) SelectedFields() []*Attribute {
	var fields fieldList
	q.Pipeline.eachElement(func(e Element) {
		if s, ok := e.(*Select); ok {
			for _, f := range s.Fields {
				fields.add(f)
			}
		}
	})
	return fields.list
}

// FilteredFields lists the attributes and intrinsics every spanset filter of
// q reads, across operators and nested pipelines, each once in first-seen order.
func (q *Query) FilteredFields() []*Attribute {
	var fields fieldList
	var read func(e FieldExpr)
	read = func(e FieldExpr) {
		switch e := e.(type) {
		case *Attribute:
			fields.add(e)
		case *BinaryOp:
			read(e.LHS)
			read(e.RHS)
		case *UnaryOp:
			read(e.Expr)
		case *NilTest:
			read(e.Expr)
		}
	}
	q.Pipeline.eachElement(func(e Element) {
		if s, ok := e.(*Selector); ok && s.Filter != nil {
			read(s.Filter)
		}
	})
	return fields.list
}

// eachElement calls visit on every element of p, descending into groups,
// nested pipelines and spanset operands.
func (p *Pipeline) eachElement(visit func(Element)) {
	var walk func(elements []Element)
	walk = func(elements []Element) {
		for _, element := range elements {
			visit(element)
			switch e := element.(type) {
			case *Group:
				walk([]Element{e.Expr})
			case *Pipeline:
				walk(e.Elements)
			case *SpansetOp:
				walk([]Element{e.LHS, e.RHS})
			}
		}
	}
	walk(p.Elements)
}

// fieldList collects fields, each name once in first-seen order.
type fieldList struct {
	list []*Attribute
	seen map[string]bool
}

func (l *fieldList) add(f *Attribute) {
	if l.seen == nil {
		l.seen = map[string]bool{}
	}
	if !l.seen[f.Name] {
		l.seen[f.Name] = true
		l.list = append(l.list, f)
	}
}
