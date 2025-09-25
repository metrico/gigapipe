package optimizer

import (
	"fmt"
	"github.com/metrico/qryn/reader/promql/promql_parser"
	"github.com/metrico/qryn/reader/promql/promql_transpiler/planner"
	prom_parser "github.com/prometheus/prometheus/promql/parser"
	"math/rand"
)

type Aggregate struct {
	gExpr    *promql_parser.Expr
	expr     *prom_parser.AggregateExpr
	selector *prom_parser.VectorSelector
}

func (v *Aggregate) Applicable(expr prom_parser.Expr) bool {
	_expr, ok := expr.(*prom_parser.AggregateExpr)
	if !ok {
		return false
	}
	if _, ok := _expr.Expr.(*prom_parser.VectorSelector); !ok {
		return false
	}
	switch _expr.Op {
	case prom_parser.SUM:
		return true
	}
	return false
}

func (v *Aggregate) Optimize(gExpr *promql_parser.Expr, expr prom_parser.Expr) (prom_parser.Expr, error) {
	v.gExpr = gExpr
	v.expr = expr.(*prom_parser.AggregateExpr)
	v.selector = v.expr.Expr.(*prom_parser.VectorSelector)
	return v.sum(), nil
}

func (v *Aggregate) sum() prom_parser.Expr {
	p := planner.AggPlanner{
		Main:   nil,
		Labels: v.expr.Grouping,
		By:     !v.expr.Without,
		Fn:     "sum",
	}

	if v.gExpr.Substitutes[v.selector.Name] != nil {
		p.Main = v.gExpr.Substitutes[v.selector.Name].Request
		delete(v.gExpr.Substitutes, v.selector.Name)
	}
	if p.Main == nil {
		var fp planner.StreamSelectPlanner
		for _, m := range v.selector.LabelMatchers {
			fp.LabelNames = append(fp.LabelNames, m.Name)
			fp.Ops = append(fp.Ops, m.Type.String())
			fp.Values = append(fp.Values, m.Value)
		}
		p.Main = &planner.DownsampleValuesPlanner{ValuesPlanner: planner.ValuesPlanner{Fp: &fp}}
	}
	metricName := fmt.Sprintf("__metric_subst__%d", rand.Int63())

	v.gExpr.Substitutes[metricName] = &promql_parser.Substitute{
		MetricName: metricName,
		Node:       v.expr,
		Request:    &p,
		Notes: promql_parser.SubstituteNotes{
			NeedsLabelsValues: false,
		},
	}
	return &prom_parser.VectorSelector{
		Name: metricName,
	}
}
