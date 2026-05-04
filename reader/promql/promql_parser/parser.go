package promql_parser

import (
	"github.com/prometheus/prometheus/promql/parser"
)

func Parse(query string) (*Expr, error) {
	expr, err := parser.NewParser(parser.Options{}).ParseExpr(query)
	return &Expr{
		Expr:        expr,
		Substitutes: make(map[string]*Substitute),
	}, err
}
