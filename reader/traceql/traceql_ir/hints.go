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
	var res []*Attribute
	seen := map[string]bool{}
	var walk func(elements []Element)
	walk = func(elements []Element) {
		for _, element := range elements {
			switch e := element.(type) {
			case *Select:
				for _, f := range e.Fields {
					if !seen[f.Name] {
						seen[f.Name] = true
						res = append(res, f)
					}
				}
			case *Group:
				walk([]Element{e.Expr})
			case *Pipeline:
				walk(e.Elements)
			case *SpansetOp:
				walk([]Element{e.LHS, e.RHS})
			}
		}
	}
	walk(q.Pipeline.Elements)
	return res
}
