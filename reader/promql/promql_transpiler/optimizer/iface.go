package optimizer

import (
	"github.com/metrico/qryn/v4/reader/promql/promql_parser"
	prom_parser "github.com/prometheus/prometheus/promql/parser"
)

type Optimizer interface {
	Applicable(expr prom_parser.Expr) bool
	Optimize(gExpr *promql_parser.Expr, expr prom_parser.Expr) (prom_parser.Expr, error)
}
