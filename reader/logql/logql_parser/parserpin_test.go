package logql_parser

import (
	"testing"

	"github.com/metrico/qryn/v5/reader/internal/goldentest"
)

// TestParserPins pins the LogQL parser's AST shape for a representative
// spread of valid query shapes, and its error behavior for a representative
// spread of invalid ones. See reader/internal/goldentest/parserpin.go for
// what RunParserPins asserts generically (non-nil root / non-nil error); the
// Check callbacks here go further for the cases where a deeper shape pin is
// worth the assertion.
func TestParserPins(t *testing.T) {
	goldentest.RunParserPins(t, Parse, []goldentest.ParserPinCase[*LogQLScript]{
		{
			Name:  "bare_stream_selector",
			Query: `{app="x"}`,
			Check: func(t *testing.T, ast *LogQLScript) {
				sel := ast.Head.StrSelector
				if sel == nil {
					t.Fatalf("expected a non-nil StrSelector head, got %+v", ast.Head)
				}
				if len(sel.StrSelCmds) != 1 || sel.StrSelCmds[0].Label.Name != "app" || sel.StrSelCmds[0].Op != "=" {
					t.Errorf("expected a single app= matcher, got %+v", sel.StrSelCmds)
				}
			},
		},
		{
			Name:  "multi_matcher_selector",
			Query: `{app="x", env=~"prod.*"}`,
			Check: func(t *testing.T, ast *LogQLScript) {
				sel := ast.Head.StrSelector
				if sel == nil || len(sel.StrSelCmds) != 2 {
					t.Fatalf("expected 2 matchers, got %+v", ast.Head)
				}
				if sel.StrSelCmds[1].Op != "=~" {
					t.Errorf("expected second matcher op =~, got %q", sel.StrSelCmds[1].Op)
				}
			},
		},
		{
			Name:  "line_filter_contains",
			Query: `{app="x"} |= "error"`,
			Check: func(t *testing.T, ast *LogQLScript) {
				sel := ast.Head.StrSelector
				if sel == nil || len(sel.Pipelines) != 1 || sel.Pipelines[0].LineFilter == nil {
					t.Fatalf("expected a single LineFilter pipeline stage, got %+v", ast.Head)
				}
				if sel.Pipelines[0].LineFilter.Fn != "|=" {
					t.Errorf("expected line filter fn |=, got %q", sel.Pipelines[0].LineFilter.Fn)
				}
			},
		},
		{
			Name:  "label_filter_numeric",
			Query: `{app="x"} | freq >= 4`,
			Check: func(t *testing.T, ast *LogQLScript) {
				sel := ast.Head.StrSelector
				if sel == nil || len(sel.Pipelines) != 1 || sel.Pipelines[0].LabelFilter == nil {
					t.Fatalf("expected a single LabelFilter pipeline stage, got %+v", ast.Head)
				}
				head := sel.Pipelines[0].LabelFilter.Head.SimpleHead
				if head == nil || head.Fn != ">=" || head.NumVal != "4" {
					t.Errorf("expected a numeric >= 4 filter, got %+v", head)
				}
			},
		},
		{
			Name:  "parser_json_with_params",
			Query: `{app="x"} | json field="path"`,
			Check: func(t *testing.T, ast *LogQLScript) {
				sel := ast.Head.StrSelector
				if sel == nil || len(sel.Pipelines) != 1 || sel.Pipelines[0].Parser == nil {
					t.Fatalf("expected a single Parser pipeline stage, got %+v", ast.Head)
				}
				p := sel.Pipelines[0].Parser
				if p.Fn != "json" || len(p.ParserParams) != 1 || p.ParserParams[0].Label.Name != "field" {
					t.Errorf("expected json parser with 1 param named field, got %+v", p)
				}
			},
		},
		{
			Name:  "parser_logfmt_bare",
			Query: `{app="x"} | logfmt`,
			Check: func(t *testing.T, ast *LogQLScript) {
				sel := ast.Head.StrSelector
				if sel == nil || len(sel.Pipelines) != 1 || sel.Pipelines[0].Parser == nil {
					t.Fatalf("expected a single Parser pipeline stage, got %+v", ast.Head)
				}
				if sel.Pipelines[0].Parser.Fn != "logfmt" {
					t.Errorf("expected logfmt parser, got %q", sel.Pipelines[0].Parser.Fn)
				}
			},
		},
		{
			Name:  "line_format",
			Query: "{app=\"x\"} | line_format `{{.path}}`",
			Check: func(t *testing.T, ast *LogQLScript) {
				sel := ast.Head.StrSelector
				if sel == nil || len(sel.Pipelines) != 1 || sel.Pipelines[0].LineFormat == nil {
					t.Fatalf("expected a single LineFormat pipeline stage, got %+v", ast.Head)
				}
			},
		},
		{
			Name:  "unwrap_both_spellings_equivalent",
			Query: `sum_over_time({app="x"} | unwrap bytes [5m])`,
			Check: func(t *testing.T, ast *LogQLScript) {
				lra := ast.Head.LRAOrUnwrap
				if lra == nil || lra.Fn != "sum_over_time" {
					t.Fatalf("expected an LRAOrUnwrap head with fn sum_over_time, got %+v", ast.Head)
				}
				pipelines := lra.StrSel.Pipelines
				if len(pipelines) != 1 || pipelines[0].Unwrap == nil {
					t.Fatalf("expected a single Unwrap pipeline stage, got %+v", pipelines)
				}
				if pipelines[0].Unwrap.Fn != "unwrap" || pipelines[0].Unwrap.Label.Name != "bytes" {
					t.Errorf("expected unwrap bytes, got %+v", pipelines[0].Unwrap)
				}
			},
		},
		{
			Name:  "range_aggregation_with_by",
			Query: `rate({app="x"}[5m]) by (path)`,
			Check: func(t *testing.T, ast *LogQLScript) {
				lra := ast.Head.LRAOrUnwrap
				if lra == nil || lra.Fn != "rate" {
					t.Fatalf("expected an LRAOrUnwrap head with fn rate, got %+v", ast.Head)
				}
				if lra.ByOrWithoutSuffix == nil || lra.ByOrWithoutSuffix.Fn != "by" {
					t.Fatalf("expected a by(...) suffix, got %+v", lra.ByOrWithoutSuffix)
				}
				if len(lra.ByOrWithoutSuffix.Labels) != 1 || lra.ByOrWithoutSuffix.Labels[0].Name != "path" {
					t.Errorf("expected by(path), got %+v", lra.ByOrWithoutSuffix.Labels)
				}
			},
		},
		{
			Name:  "aggregation_sum_by",
			Query: `sum by (path) (rate({app="x"}[1m]))`,
			Check: func(t *testing.T, ast *LogQLScript) {
				agg := ast.Head.AggOperator
				if agg == nil || agg.Fn != "sum" {
					t.Fatalf("expected an AggOperator head with fn sum, got %+v", ast.Head)
				}
				if agg.ByOrWithoutPrefix == nil || agg.ByOrWithoutPrefix.Fn != "by" {
					t.Errorf("expected a by(...) prefix, got %+v", agg.ByOrWithoutPrefix)
				}
			},
		},
		{
			Name:  "aggregation_without",
			Query: `sum without (instance) (rate({app="x"}[1m]))`,
			Check: func(t *testing.T, ast *LogQLScript) {
				agg := ast.Head.AggOperator
				if agg == nil || agg.ByOrWithoutPrefix == nil || agg.ByOrWithoutPrefix.Fn != "without" {
					t.Fatalf("expected a without(...) prefix, got %+v", ast.Head)
				}
			},
		},
		{
			Name:  "topk",
			Query: `topk(5, sum by (path) (rate({app="x"}[5m])))`,
			Check: func(t *testing.T, ast *LogQLScript) {
				topK := ast.Head.TopK
				if topK == nil || topK.Fn != "topk" || topK.Param != "5" {
					t.Fatalf("expected a topk(5, ...) head, got %+v", ast.Head)
				}
				if topK.AggOperator == nil {
					t.Errorf("expected the topk argument to be an AggOperator, got %+v", topK)
				}
			},
		},
		{
			Name:  "binary_expression",
			Query: `rate({app="a"}[1m]) / rate({app="b"}[1m])`,
			Check: func(t *testing.T, ast *LogQLScript) {
				if !ast.IsBinary() {
					t.Fatalf("expected IsBinary()==true, got BinOps=%+v", ast.BinOps)
				}
				if len(ast.BinOps) != 1 || ast.BinOps[0].Op != "/" {
					t.Errorf("expected a single '/' binary op, got %+v", ast.BinOps)
				}
			},
		},

		// Invalid queries: only a non-nil parse error is asserted (see
		// RunParserPins), spread across a representative range of syntax
		// error classes.
		{Name: "invalid_unclosed_brace", Query: `{app="x"`, WantErr: true},
		{Name: "invalid_trailing_operator", Query: `rate({app="x"}[1m]) /`, WantErr: true},
		{Name: "invalid_unterminated_string", Query: `{app="x} |= "error"`, WantErr: true},
		{Name: "invalid_range_duration_unit", Query: `rate({app="x"}[5x])`, WantErr: true},
		{Name: "invalid_missing_matcher_op", Query: `{app "x"}`, WantErr: true},
		{Name: "invalid_unclosed_range_call", Query: `rate({app="x"}[5m]`, WantErr: true},
	})
}
