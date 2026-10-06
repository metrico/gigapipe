package optimizer

import (
	"fmt"
	"time"

	"github.com/metrico/qryn/v5/reader/promql/metricread"
	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
	"github.com/prometheus/prometheus/model/labels"
	prom_parser "github.com/prometheus/prometheus/promql/parser"
)

// Pushdown substitutes a pushed-down range function, instant selector, or one aggregation
// wrapping either, evaluated at the query's Grid. An instant selector reads Lookback back.
type Pushdown struct {
	Grid     metricread.Grid
	Lookback time.Duration
}

func (p *Pushdown) Applicable(expr prom_parser.Expr) bool {
	_, ok := p.pushdown(expr)
	return ok
}

func (p *Pushdown) Optimize(gExpr *promql_parser.Expr, expr prom_parser.Expr) (prom_parser.Expr, error) {
	pd, ok := p.pushdown(expr)
	if !ok {
		return expr, nil
	}
	name := fmt.Sprintf("__metric_subst__%d", len(gExpr.Substitutes)+1)
	gExpr.Substitutes[name] = &promql_parser.Substitute{MetricName: name, Node: expr, Pushdown: pd.Pushdown}
	return substituteSelector(pd.selector, name), nil
}

type pushdown struct {
	metricread.Pushdown
	selector *prom_parser.VectorSelector
}

func (p *Pushdown) pushdown(expr prom_parser.Expr) (pushdown, bool) {
	agg, ok := unwrap(expr).(*prom_parser.AggregateExpr)
	if !ok {
		return p.leaf(expr)
	}
	if agg.Param != nil || !metricread.Aggregable(agg.Op.String()) {
		return pushdown{}, false
	}
	pd, ok := p.leaf(agg.Expr)
	if ok && !metricread.KeepsName(pd.Func) && !namesOne(pd.Matchers) {
		// Series of several names may share a label set once __name__ is dropped, which the
		// engine rejects before aggregating.
		return pushdown{}, false
	}
	pd.Aggregation = &metricread.Aggregation{Op: agg.Op.String(), Grouping: agg.Grouping, Without: agg.Without}
	return pd, ok
}

// leaf matches a pushable range function over a matrix selector, or an instant selector.
func (p *Pushdown) leaf(expr prom_parser.Expr) (pushdown, bool) {
	switch n := unwrap(expr).(type) {
	case *prom_parser.VectorSelector:
		return pushdown{Pushdown: metricread.Pushdown{Grid: p.Grid, RangeMs: p.Lookback.Milliseconds(),
			Matchers: n.LabelMatchers}, selector: n}, onGrid(n)
	case *prom_parser.Call:
		if !metricread.Pushable(n.Func.Name) || len(n.Args) != 1 {
			return pushdown{}, false
		}
		ms, ok := unwrap(n.Args[0]).(*prom_parser.MatrixSelector)
		if !ok {
			return pushdown{}, false
		}
		vs, ok := ms.VectorSelector.(*prom_parser.VectorSelector)
		if !ok {
			return pushdown{}, false
		}
		return pushdown{Pushdown: metricread.Pushdown{Grid: p.Grid, Func: n.Func.Name, RangeMs: ms.Range.Milliseconds(),
			Matchers: vs.LabelMatchers}, selector: vs}, onGrid(vs)
	}
	return pushdown{}, false
}

// onGrid reports whether the selector reads at the query's own timestamps: no offset, no @
// and no anchored or smoothed modifier.
func onGrid(vs *prom_parser.VectorSelector) bool {
	return vs.OriginalOffset == 0 && vs.Timestamp == nil && vs.StartOrEnd == 0 && !vs.Anchored && !vs.Smoothed
}

// namesOne reports whether the matchers select a single metric name.
func namesOne(matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		if m.Name == labels.MetricName && m.Type == labels.MatchEqual {
			return true
		}
	}
	return false
}

func unwrap(expr prom_parser.Expr) prom_parser.Expr {
	for {
		paren, ok := expr.(*prom_parser.ParenExpr)
		if !ok {
			return expr
		}
		expr = paren.Expr
	}
}
