package parser

import "testing"

func TestParser(t *testing.T) {
	p, err := Parse("rate(http_requests_total{status=\"5xx\"}[5m])")
	if err != nil {
		t.Fatal(err)
	}
	print(p.Expr.String())
}

/*
parser.Call
  Func:
    name: "rate"
  Args:
  - parser.MatrixSelector:
    VectorSelector: parser.VectorSelector:
      Name: http_requests_total
      LabelMatchers:
      - bla-bla-bla
*/
