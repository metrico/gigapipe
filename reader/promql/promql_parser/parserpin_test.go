package promql_parser

import (
	"testing"

	"github.com/metrico/qryn/v5/reader/internal/parserpin"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

// TestParserPins pins the PromQL parser's AST shape for a representative
// spread of valid query shapes, and its error behavior for a representative
// spread of invalid ones. See reader/internal/parserpin/parserpin.go for
// what RunParserPins asserts generically (non-nil root / non-nil error); the
// Check callbacks here go further for the cases where a deeper shape pin is
// worth the assertion.
func TestParserPins(t *testing.T) {
	parserpin.RunParserPins(t, Parse, []parserpin.ParserPinCase[*Expr]{
		{
			Name:  "bare_selector",
			Query: `http_requests_total`,
			Check: func(t *testing.T, ast *Expr) {
				vs, ok := ast.Expr.(*parser.VectorSelector)
				if !ok {
					t.Fatalf("expected *parser.VectorSelector, got %T", ast.Expr)
				}
				if vs.Name != "http_requests_total" {
					t.Errorf("expected metric name %q, got %q", "http_requests_total", vs.Name)
				}
			},
		},
		{
			Name:  "selector_with_matchers",
			Query: `http_requests_total{job="api",method!="GET"}`,
			Check: func(t *testing.T, ast *Expr) {
				vs, ok := ast.Expr.(*parser.VectorSelector)
				if !ok {
					t.Fatalf("expected *parser.VectorSelector, got %T", ast.Expr)
				}
				// __name__ plus the two explicit matchers.
				if len(vs.LabelMatchers) != 3 {
					t.Errorf("expected 3 label matchers (incl. __name__), got %d", len(vs.LabelMatchers))
				}
			},
		},
		{
			Name:  "selector_with_regex_matcher",
			Query: `http_requests_total{job=~"api.*"}`,
			Check: func(t *testing.T, ast *Expr) {
				vs, ok := ast.Expr.(*parser.VectorSelector)
				if !ok {
					t.Fatalf("expected *parser.VectorSelector, got %T", ast.Expr)
				}
				found := false
				for _, m := range vs.LabelMatchers {
					if m.Name == "job" && m.Type == labels.MatchRegexp {
						found = true
					}
				}
				if !found {
					t.Errorf("expected a regex matcher on job, got %v", vs.LabelMatchers)
				}
			},
		},
		{
			Name:  "range_vector_selector",
			Query: `http_requests_total[5m]`,
			Check: func(t *testing.T, ast *Expr) {
				ms, ok := ast.Expr.(*parser.MatrixSelector)
				if !ok {
					t.Fatalf("expected *parser.MatrixSelector, got %T", ast.Expr)
				}
				if ms.Range.String() != "5m0s" {
					t.Errorf("expected range 5m0s, got %s", ms.Range)
				}
			},
		},
		{
			Name:  "range_function_call",
			Query: `rate(http_requests_total[5m])`,
			Check: func(t *testing.T, ast *Expr) {
				call, ok := ast.Expr.(*parser.Call)
				if !ok {
					t.Fatalf("expected *parser.Call, got %T", ast.Expr)
				}
				if call.Func.Name != "rate" {
					t.Errorf("expected func name rate, got %s", call.Func.Name)
				}
			},
		},
		{
			Name:  "nested_function_calls",
			Query: `histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))`,
			Check: func(t *testing.T, ast *Expr) {
				call, ok := ast.Expr.(*parser.Call)
				if !ok {
					t.Fatalf("expected *parser.Call, got %T", ast.Expr)
				}
				if call.Func.Name != "histogram_quantile" {
					t.Errorf("expected func name histogram_quantile, got %s", call.Func.Name)
				}
				if len(call.Args) != 2 {
					t.Fatalf("expected 2 args, got %d", len(call.Args))
				}
				if _, ok := call.Args[1].(*parser.Call); !ok {
					t.Errorf("expected second arg to be a nested *parser.Call, got %T", call.Args[1])
				}
			},
		},
		{
			Name:  "aggregation_by",
			Query: `sum by (job) (http_requests_total)`,
			Check: func(t *testing.T, ast *Expr) {
				agg, ok := ast.Expr.(*parser.AggregateExpr)
				if !ok {
					t.Fatalf("expected *parser.AggregateExpr, got %T", ast.Expr)
				}
				if agg.Without {
					t.Errorf("expected 'by' grouping, got 'without'")
				}
				if len(agg.Grouping) != 1 || agg.Grouping[0] != "job" {
					t.Errorf("expected grouping [job], got %v", agg.Grouping)
				}
			},
		},
		{
			Name:  "aggregation_without",
			Query: `sum without (instance) (http_requests_total)`,
			Check: func(t *testing.T, ast *Expr) {
				agg, ok := ast.Expr.(*parser.AggregateExpr)
				if !ok {
					t.Fatalf("expected *parser.AggregateExpr, got %T", ast.Expr)
				}
				if !agg.Without {
					t.Errorf("expected 'without' grouping, got 'by'")
				}
			},
		},
		{
			Name:  "aggregation_with_param",
			Query: `topk(5, http_requests_total)`,
			Check: func(t *testing.T, ast *Expr) {
				agg, ok := ast.Expr.(*parser.AggregateExpr)
				if !ok {
					t.Fatalf("expected *parser.AggregateExpr, got %T", ast.Expr)
				}
				if agg.Op != parser.TOPK {
					t.Errorf("expected op TOPK, got %s", agg.Op)
				}
				if agg.Param == nil {
					t.Errorf("expected a non-nil Param (k)")
				}
			},
		},
		{
			Name:  "binary_expression",
			Query: `http_requests_total{job="a"} / http_requests_total{job="b"}`,
			Check: func(t *testing.T, ast *Expr) {
				bin, ok := ast.Expr.(*parser.BinaryExpr)
				if !ok {
					t.Fatalf("expected *parser.BinaryExpr, got %T", ast.Expr)
				}
				if bin.Op != parser.DIV {
					t.Errorf("expected op DIV, got %s", bin.Op)
				}
			},
		},
		{
			Name:  "binary_bool_comparison",
			Query: `up{job="api"} == bool 1`,
			Check: func(t *testing.T, ast *Expr) {
				bin, ok := ast.Expr.(*parser.BinaryExpr)
				if !ok {
					t.Fatalf("expected *parser.BinaryExpr, got %T", ast.Expr)
				}
				if bin.ReturnBool != true {
					t.Errorf("expected ReturnBool=true for 'bool' modifier")
				}
			},
		},
		{
			Name:  "offset_modifier",
			Query: `http_requests_total offset 5m`,
			Check: func(t *testing.T, ast *Expr) {
				vs, ok := ast.Expr.(*parser.VectorSelector)
				if !ok {
					t.Fatalf("expected *parser.VectorSelector, got %T", ast.Expr)
				}
				if vs.OriginalOffset.String() != "5m0s" {
					t.Errorf("expected offset 5m0s, got %s", vs.OriginalOffset)
				}
			},
		},
		{
			Name:  "subquery",
			Query: `rate(http_requests_total[5m])[30m:1m]`,
			Check: func(t *testing.T, ast *Expr) {
				sq, ok := ast.Expr.(*parser.SubqueryExpr)
				if !ok {
					t.Fatalf("expected *parser.SubqueryExpr, got %T", ast.Expr)
				}
				if sq.Range.String() != "30m0s" {
					t.Errorf("expected subquery range 30m0s, got %s", sq.Range)
				}
			},
		},
		{
			Name:  "unary_expression",
			Query: `-(http_requests_total)`,
			Check: func(t *testing.T, ast *Expr) {
				if _, ok := ast.Expr.(*parser.UnaryExpr); !ok {
					t.Fatalf("expected *parser.UnaryExpr, got %T", ast.Expr)
				}
			},
		},

		// Invalid queries: only a non-nil parse error is asserted (see
		// RunParserPins), spread across a representative range of syntax
		// error classes.
		{Name: "invalid_unclosed_brace", Query: `http_requests_total{job="api"`, WantErr: true},
		{Name: "invalid_trailing_operator", Query: `http_requests_total +`, WantErr: true},
		{Name: "invalid_unterminated_string", Query: `http_requests_total{job="api}`, WantErr: true},
		{Name: "invalid_range_duration", Query: `rate(http_requests_total[5x])`, WantErr: true},
		{Name: "invalid_aggregation_syntax", Query: `sum(by (job) http_requests_total)`, WantErr: true},
		{Name: "invalid_unclosed_call", Query: `rate(http_requests_total[5m]`, WantErr: true},
	})
}
