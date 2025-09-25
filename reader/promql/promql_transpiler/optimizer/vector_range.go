package optimizer

import (
	"fmt"
	"github.com/metrico/qryn/reader/logql/logql_transpiler/clickhouse_planner"
	"github.com/metrico/qryn/reader/promql/promql_parser"
	"github.com/metrico/qryn/reader/promql/promql_transpiler/planner"
	prom_parser "github.com/prometheus/prometheus/promql/parser"
	"math/rand"
)

type VectorRange struct {
	gExpr *promql_parser.Expr
	expr  prom_parser.Expr

	selector *prom_parser.MatrixSelector
	fn       string
}

func (v *VectorRange) Applicable(expr prom_parser.Expr) bool {
	_expr, ok := expr.(*prom_parser.Call)
	if !ok {
		return false
	}
	if len(_expr.Args) != 1 {
		return false
	}
	_, ok = _expr.Args[0].(*prom_parser.MatrixSelector)
	if !ok {
		return false
	}
	switch _expr.Func.Name {
	case "rate":
		return true
	case "sum_over_time":
	case "min_over_time":
	case "max_over_time":
	case "count_over_time":
	case "stddev_over_time":
	case "stdvar_over_time":
	case "last_over_time":
	case "avg_over_time":
	}
	return false
}

func (v *VectorRange) Optimize(gExpr *promql_parser.Expr, expr prom_parser.Expr) (prom_parser.Expr, error) {
	v.gExpr = gExpr
	v.expr = expr
	_expr := v.expr.(*prom_parser.Call)
	v.selector = _expr.Args[0].(*prom_parser.MatrixSelector)
	v.fn = _expr.Func.Name
	switch v.fn {
	case "rate":
		return v.rate(), nil
	case "sum_over_time":
	case "min_over_time":
	case "max_over_time":
	case "count_over_time":
	case "stddev_over_time":
	case "stdvar_over_time":
	case "last_over_time":
	case "avg_over_time":
	}
	return v.expr, nil
}

func (v *VectorRange) rate() prom_parser.Expr {
	fpPlanner := &planner.StreamSelectPlanner{
		clickhouse_planner.StreamSelectPlanner{
			LabelNames: nil,
			Ops:        nil,
			Values:     nil,
		},
	}

	strSelect := v.selector.VectorSelector.(*prom_parser.VectorSelector)
	for _, lm := range strSelect.LabelMatchers {
		fpPlanner.LabelNames = append(fpPlanner.LabelNames, lm.Name)
		fpPlanner.Ops = append(fpPlanner.Ops, lm.Type.String())
		fpPlanner.Values = append(fpPlanner.Values, lm.Value)
	}

	ratePlanner := &planner.RatePlanner{
		FpPlanner: fpPlanner,
		Duration:  v.selector.Range,
	}

	metricName := fmt.Sprintf("__metric_subst__%d", rand.Int63())

	v.gExpr.Substitutes[metricName] = &promql_parser.Substitute{
		MetricName: metricName,
		Node:       v.expr,
		Request:    ratePlanner,
		Notes: promql_parser.SubstituteNotes{
			NeedsLabelsValues: true,
		},
	}
	return &prom_parser.VectorSelector{
		Name: metricName,
	}
}
