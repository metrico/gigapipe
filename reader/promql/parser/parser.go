package parser

import (
	"fmt"
	"github.com/prometheus/prometheus/promql/parser"
)

func Parse(query string) (Node, error) {
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return nil, err
	}
	switch expr.(type) {
	case *parser.VectorSelector:
		return &VectorSelector{node: expr.(*parser.VectorSelector)}, nil
	}
	return nil, fmt.Errorf("%T not supported", expr)
}
