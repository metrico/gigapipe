package promql_transpiler

import (
	"fmt"
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

func TestEarliestReadNS(t *testing.T) {
	start := time.UnixMilli(1_700_000_000_000)
	atMs := int64(1_600_000_000_000)
	cases := []struct {
		query string
		want  int64
	}{
		{"up", start.Add(-5 * time.Minute).UnixNano()},
		{"rate(a[5m])", start.Add(-10 * time.Minute).UnixNano()},
		// A literal @ before the query start moves the earliest read back.
		{fmt.Sprintf("rate(a[5m] @ %d)", atMs/1000),
			time.UnixMilli(atMs).Add(-10 * time.Minute).UnixNano()},
		// @ after the query start does not move it forward.
		{fmt.Sprintf("rate(a[5m] @ %d)", (start.UnixMilli()+3_600_000)/1000),
			start.Add(-10 * time.Minute).UnixNano()},
		// @ start()/end() never precede the query start.
		{"rate(a[5m] @ start())", start.Add(-10 * time.Minute).UnixNano()},
		{fmt.Sprintf("max_over_time((a @ %d)[1h:15s])", atMs/1000),
			time.UnixMilli(atMs).Add(-65 * time.Minute).UnixNano()},
	}
	for _, c := range cases {
		expr, err := promql_parser.Parse(c.query)
		if err != nil {
			t.Fatalf("%s: %v", c.query, err)
		}
		if got := EarliestReadNS(expr.Expr, start); got != c.want {
			t.Errorf("%s: got %d, want %d", c.query, got, c.want)
		}
	}
}
