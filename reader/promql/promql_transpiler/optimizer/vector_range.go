package optimizer

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/clickhouse_planner"
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v4/reader/promql/promql_parser"
	"github.com/metrico/qryn/v4/reader/promql/promql_transpiler/planner"
	prom_parser "github.com/prometheus/prometheus/promql/parser"
)

// rangeFns maps every accelerated range function to the planner that builds it.
//
// The plain aggregates over (t-range, t] go to OverTimePlanner. The ones that
// measure across the frame boundary need the counter machinery instead: rate,
// increase and delta compare the endpoints of the range, resets and changes
// count transitions between consecutive samples.
var rangeFns = map[string]func(fp shared.SQLRequestPlanner, d time.Duration, fn string) shared.SQLRequestPlanner{
	"sum_over_time":     newOverTime,
	"count_over_time":   newOverTime,
	"min_over_time":     newOverTime,
	"max_over_time":     newOverTime,
	"avg_over_time":     newOverTime,
	"last_over_time":    newOverTime,
	"present_over_time": newOverTime,

	"rate":     newCounter,
	"increase": newCounter,
	"delta":    newCounter,

	"resets":  newCounterFlags,
	"changes": newCounterFlags,
}

func newOverTime(fp shared.SQLRequestPlanner, d time.Duration, fn string) shared.SQLRequestPlanner {
	return &planner.OverTimePlanner{FpPlanner: fp, Duration: d, Fn: fn}
}

func newCounter(fp shared.SQLRequestPlanner, d time.Duration, fn string) shared.SQLRequestPlanner {
	return &planner.CounterPlanner{FpPlanner: fp, Duration: d, Fn: fn}
}

func newCounterFlags(fp shared.SQLRequestPlanner, d time.Duration, fn string) shared.SQLRequestPlanner {
	return &planner.CounterFlagsPlanner{FpPlanner: fp, Duration: d, Fn: fn}
}

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
	if _, ok = _expr.Args[0].(*prom_parser.MatrixSelector); !ok {
		return false
	}
	_, ok = rangeFns[_expr.Func.Name]
	return ok
}

func (v *VectorRange) Optimize(gExpr *promql_parser.Expr, expr prom_parser.Expr) (prom_parser.Expr, error) {
	v.gExpr = gExpr
	v.expr = expr
	_expr := v.expr.(*prom_parser.Call)
	v.selector = _expr.Args[0].(*prom_parser.MatrixSelector)
	v.fn = _expr.Func.Name

	build, ok := rangeFns[v.fn]
	if !ok {
		return v.expr, nil
	}
	return v.substitute(build(v.fpPlanner(), v.selector.Range, v.fn)), nil
}

func (v *VectorRange) fpPlanner() shared.SQLRequestPlanner {
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
	return fpPlanner
}

// substitute swaps the call out for a synthetic vector selector and registers
// the planner that produces it.
func (v *VectorRange) substitute(p shared.SQLRequestPlanner) prom_parser.Expr {
	metricName := fmt.Sprintf("__metric_subst__%d", rand.Int63())
	v.gExpr.Substitutes[metricName] = &promql_parser.Substitute{
		MetricName: metricName,
		Node:       v.expr,
		Request:    p,
		Notes: promql_parser.SubstituteNotes{
			NeedsLabelsValues: true,
		},
	}
	return &prom_parser.VectorSelector{
		Name: metricName,
	}
}
