package optimizer

import (
	"fmt"
	"math/rand"

	"github.com/metrico/qryn/v4/reader/promql/promql_parser"
	"github.com/metrico/qryn/v4/reader/promql/promql_transpiler/planner"
	prom_parser "github.com/prometheus/prometheus/promql/parser"
)

// aggFns maps the prometheus cross-series aggregation operators we accelerate to
// the AggPlanner function name that implements each. topk/bottomk (which keep the
// input series) and quantile/count_values (which need bespoke shapes) are absent
// and fall back to the engine.
var aggFns = map[prom_parser.ItemType]string{
	prom_parser.SUM:    "sum",
	prom_parser.MIN:    "min",
	prom_parser.MAX:    "max",
	prom_parser.AVG:    "avg",
	prom_parser.COUNT:  "count",
	prom_parser.GROUP:  "group",
	prom_parser.STDDEV: "stddev",
	prom_parser.STDVAR: "stdvar",
}

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
	_, ok = aggFns[_expr.Op]
	return ok
}

func (v *Aggregate) Optimize(gExpr *promql_parser.Expr, expr prom_parser.Expr) (prom_parser.Expr, error) {
	v.gExpr = gExpr
	v.expr = expr.(*prom_parser.AggregateExpr)
	v.selector = v.expr.Expr.(*prom_parser.VectorSelector)
	return v.aggregate(aggFns[v.expr.Op]), nil
}

func (v *Aggregate) aggregate(fn string) prom_parser.Expr {
	p := planner.AggPlanner{
		Labels: v.expr.Grouping,
		By:     !v.expr.Without,
		Fn:     fn,
	}

	if sub := v.gExpr.Substitutes[v.selector.Name]; sub != nil {
		// The inner expression was already accelerated. We can aggregate a
		// per-series producer (rate, *_over_time, ...), but not another
		// cross-series aggregation: its output is a relabeled, combined series
		// with no fingerprint subquery to group on. Leave the outer aggregation
		// for the engine, which reads the inner substitute as an instant vector.
		if _, isAgg := sub.Request.(*planner.AggPlanner); isAgg {
			return v.expr
		}
		p.Main = sub.Request
		delete(v.gExpr.Substitutes, v.selector.Name)
	}

	if p.Main == nil {
		var fp planner.StreamSelectPlanner
		for _, m := range v.selector.LabelMatchers {
			fp.LabelNames = append(fp.LabelNames, m.Name)
			fp.Ops = append(fp.Ops, m.Type.String())
			fp.Values = append(fp.Values, m.Value)
		}
		// A bare instant vector: carry each series forward 5m before combining,
		// so out-of-phase series all contribute at every step rather than
		// sawtoothing as their raw samples land on different steps.
		p.Main = planner.NewInstantVectorPlanner(&fp)
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
