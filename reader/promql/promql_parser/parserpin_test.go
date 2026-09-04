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
		{
			Name:  "label_replace_call",
			Query: `label_replace(http_requests_total, "job", "$1-$2", "instance", "local(.*):(.*)")`,
			Check: func(t *testing.T, ast *Expr) {
				call, ok := ast.Expr.(*parser.Call)
				if !ok {
					t.Fatalf("expected *parser.Call, got %T", ast.Expr)
				}
				if call.Func.Name != "label_replace" {
					t.Errorf("expected func name label_replace, got %s", call.Func.Name)
				}
				if len(call.Args) != 5 {
					t.Fatalf("expected 5 args, got %d", len(call.Args))
				}
			},
		},
		{
			Name:  "label_join_call",
			Query: `label_join(http_requests_total, "job", "-", "instance")`,
			Check: func(t *testing.T, ast *Expr) {
				call, ok := ast.Expr.(*parser.Call)
				if !ok {
					t.Fatalf("expected *parser.Call, got %T", ast.Expr)
				}
				if call.Func.Name != "label_join" {
					t.Errorf("expected func name label_join, got %s", call.Func.Name)
				}
			},
		},
		{
			// Distinct from "nested_function_calls": the second arg is a bare
			// selector, not a nested Call, so histogram_quantile's arg shape
			// isn't always "instant-vector-producing function call".
			Name:  "histogram_quantile_over_bare_selector",
			Query: `histogram_quantile(0.9, http_requests_total)`,
			Check: func(t *testing.T, ast *Expr) {
				call, ok := ast.Expr.(*parser.Call)
				if !ok {
					t.Fatalf("expected *parser.Call, got %T", ast.Expr)
				}
				if _, ok := call.Args[0].(*parser.NumberLiteral); !ok {
					t.Errorf("expected first arg to be a *parser.NumberLiteral, got %T", call.Args[0])
				}
				if _, ok := call.Args[1].(*parser.VectorSelector); !ok {
					t.Errorf("expected second arg to be a *parser.VectorSelector, got %T", call.Args[1])
				}
			},
		},
		{
			Name:  "zero_arg_call",
			Query: `time()`,
			Check: func(t *testing.T, ast *Expr) {
				call, ok := ast.Expr.(*parser.Call)
				if !ok {
					t.Fatalf("expected *parser.Call, got %T", ast.Expr)
				}
				if call.Func.Name != "time" {
					t.Errorf("expected func name time, got %s", call.Func.Name)
				}
				if len(call.Args) != 0 {
					t.Errorf("expected 0 args, got %d", len(call.Args))
				}
			},
		},
		{
			Name:  "scalar_bool_comparison_against_call",
			Query: `1 >= bool time()`,
			Check: func(t *testing.T, ast *Expr) {
				bin, ok := ast.Expr.(*parser.BinaryExpr)
				if !ok {
					t.Fatalf("expected *parser.BinaryExpr, got %T", ast.Expr)
				}
				if bin.Op != parser.GTE {
					t.Errorf("expected op GTE, got %s", bin.Op)
				}
				if !bin.ReturnBool {
					t.Errorf("expected ReturnBool=true for 'bool' modifier")
				}
				if _, ok := bin.LHS.(*parser.NumberLiteral); !ok {
					t.Errorf("expected LHS to be a *parser.NumberLiteral, got %T", bin.LHS)
				}
			},
		},
		{
			// Power binds tighter than unary minus: the top node is the
			// negation, wrapping the exponentiation, not the reverse.
			Name:  "unary_minus_binds_looser_than_power",
			Query: `-1 ^ 2`,
			Check: func(t *testing.T, ast *Expr) {
				un, ok := ast.Expr.(*parser.UnaryExpr)
				if !ok {
					t.Fatalf("expected *parser.UnaryExpr, got %T", ast.Expr)
				}
				if _, ok := un.Expr.(*parser.BinaryExpr); !ok {
					t.Errorf("expected the unary's inner expr to be a *parser.BinaryExpr (the ^), got %T", un.Expr)
				}
			},
		},
		{
			Name:  "aggregation_without_multiple_labels",
			Query: `avg without (instance, type) (http_requests_total)`,
			Check: func(t *testing.T, ast *Expr) {
				agg, ok := ast.Expr.(*parser.AggregateExpr)
				if !ok {
					t.Fatalf("expected *parser.AggregateExpr, got %T", ast.Expr)
				}
				if len(agg.Grouping) != 2 {
					t.Errorf("expected 2 grouping labels, got %v", agg.Grouping)
				}
			},
		},
		{
			Name:  "topk_bottomk_empty_without",
			Query: `bottomk without () (2, http_requests_total)`,
			Check: func(t *testing.T, ast *Expr) {
				agg, ok := ast.Expr.(*parser.AggregateExpr)
				if !ok {
					t.Fatalf("expected *parser.AggregateExpr, got %T", ast.Expr)
				}
				if agg.Op != parser.BOTTOMK {
					t.Errorf("expected op BOTTOMK, got %s", agg.Op)
				}
				if !agg.Without || len(agg.Grouping) != 0 {
					t.Errorf("expected empty 'without' grouping, got without=%v grouping=%v", agg.Without, agg.Grouping)
				}
				if agg.Param == nil {
					t.Errorf("expected a non-nil Param (k)")
				}
			},
		},
		{
			Name:  "quantile_aggregation",
			Query: `quantile(0.9, http_requests_total)`,
			Check: func(t *testing.T, ast *Expr) {
				agg, ok := ast.Expr.(*parser.AggregateExpr)
				if !ok {
					t.Fatalf("expected *parser.AggregateExpr, got %T", ast.Expr)
				}
				if agg.Op != parser.QUANTILE {
					t.Errorf("expected op QUANTILE, got %s", agg.Op)
				}
				if agg.Param == nil {
					t.Errorf("expected a non-nil Param (the quantile)")
				}
			},
		},
		{
			Name:  "unary_expr_as_call_arg",
			Query: `ceil(-http_requests_total)`,
			Check: func(t *testing.T, ast *Expr) {
				call, ok := ast.Expr.(*parser.Call)
				if !ok {
					t.Fatalf("expected *parser.Call, got %T", ast.Expr)
				}
				if _, ok := call.Args[0].(*parser.UnaryExpr); !ok {
					t.Errorf("expected the arg to be a *parser.UnaryExpr, got %T", call.Args[0])
				}
			},
		},
		{
			Name:  "vector_not_equal_call",
			Query: `http_requests_total != time()`,
			Check: func(t *testing.T, ast *Expr) {
				bin, ok := ast.Expr.(*parser.BinaryExpr)
				if !ok {
					t.Fatalf("expected *parser.BinaryExpr, got %T", ast.Expr)
				}
				if bin.Op != parser.NEQ {
					t.Errorf("expected op NEQ, got %s", bin.Op)
				}
				if _, ok := bin.RHS.(*parser.Call); !ok {
					t.Errorf("expected RHS to be a *parser.Call, got %T", bin.RHS)
				}
			},
		},
		{
			// The RHS is parenthesized, so the binary's RHS operand is the
			// *parser.ParenExpr itself, not the bool-comparison it wraps.
			Name:  "binary_with_parenthesized_bool_comparison_operand",
			Query: `http_requests_total + (1 == bool 2)`,
			Check: func(t *testing.T, ast *Expr) {
				bin, ok := ast.Expr.(*parser.BinaryExpr)
				if !ok {
					t.Fatalf("expected *parser.BinaryExpr, got %T", ast.Expr)
				}
				paren, ok := bin.RHS.(*parser.ParenExpr)
				if !ok {
					t.Fatalf("expected RHS to be a *parser.ParenExpr, got %T", bin.RHS)
				}
				if _, ok := paren.Expr.(*parser.BinaryExpr); !ok {
					t.Errorf("expected the paren's inner expr to be a *parser.BinaryExpr, got %T", paren.Expr)
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
		// A selector where every matcher (here the only one, __name__) can
		// match the empty string is rejected: PromQL requires at least one
		// non-empty-matching matcher, or the selector would match everything.
		{Name: "invalid_selector_all_matchers_match_empty", Query: `{__name__=~".*"}`, WantErr: true},
	})
}
