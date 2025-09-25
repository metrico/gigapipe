package promql_parser

import "testing"

func TestParser(t *testing.T) {
	p, err := Parse("rate(http_requests_total{status=\"5xx\"}[5m])")
	if err != nil {
		t.Fatal(err)
	}
	print(p.Expr.String())
}
