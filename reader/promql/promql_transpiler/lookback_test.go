package promql_transpiler

import (
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/promql/promql_parser"
)

func TestMaxLookback(t *testing.T) {
	cases := []struct {
		query string
		want  time.Duration
	}{
		{"up", 5 * time.Minute},
		{"rate(http_requests_total[5m])", 10 * time.Minute},
		{"sum(rate(a[1h])) / sum(rate(b[5m]))", 65 * time.Minute},
		{"rate(http_requests_total[5m] offset 30m)", 40 * time.Minute},
		{"max_over_time(rate(a[2m])[1h:15s])", 67 * time.Minute},
		{"up offset 1h", 65 * time.Minute},
	}
	for _, c := range cases {
		expr, err := promql_parser.Parse(c.query)
		if err != nil {
			t.Fatalf("%s: %v", c.query, err)
		}
		if got := MaxLookback(expr.Expr); got != c.want {
			t.Errorf("%s: got %v, want %v", c.query, got, c.want)
		}
	}
}
