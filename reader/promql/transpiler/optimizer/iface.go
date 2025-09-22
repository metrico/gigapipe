package optimizer

import (
	"github.com/metrico/qryn/reader/promql/parser"
	prom_parser "github.com/prometheus/prometheus/promql/parser"
)

type Optimizer interface {
	Applicable(expr prom_parser.Expr) bool
	Optimize(gExpr *parser.Expr, expr prom_parser.Expr) (prom_parser.Expr, error)
}
