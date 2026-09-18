package traceql_parser

import (
	"testing"

	"github.com/metrico/qryn/v5/reader/internal/parserpin"
)

// TestParserPins pins the TraceQL parser's AST shape for representative
// valid queries, and pins that a representative set of malformed queries
// fail to parse. See reader/traceql/traceql_parser/fuzz_test.go for the
// complementary "never panics" fuzz target.
func TestParserPins(t *testing.T) {
	parserpin.RunParserPins(t, Parse, []parserpin.ParserPinCase[*TraceQLScript]{
		{
			Name:  "dot_attribute_selector",
			Query: `{.http.method="GET"}`,
			Check: func(t *testing.T, ast *TraceQLScript) {
				head := ast.Head.AttrSelector.Head
				if head == nil {
					t.Fatalf("expected a non-nil attribute selector head")
				}
				if head.Label != ".http.method" {
					t.Errorf("Label = %q, want %q", head.Label, ".http.method")
				}
				if head.Op != "=" {
					t.Errorf("Op = %q, want %q", head.Op, "=")
				}
				if head.Val.StrVal == nil || head.Val.StrVal.Str != `"GET"` {
					t.Errorf("Val.StrVal = %+v, want \"GET\"", head.Val.StrVal)
				}
			},
		},
		{
			Name:  "span_prefixed_numeric",
			Query: `{span.http.status_code=200}`,
			Check: func(t *testing.T, ast *TraceQLScript) {
				head := ast.Head.AttrSelector.Head
				if head == nil {
					t.Fatalf("expected a non-nil attribute selector head")
				}
				if head.Label != "span.http.status_code" {
					t.Errorf("Label = %q, want %q", head.Label, "span.http.status_code")
				}
				if head.Val.FVal != "200" {
					t.Errorf("Val.FVal = %q, want %q", head.Val.FVal, "200")
				}
			},
		},
		{
			Name:  "intrinsic_duration_comparison",
			Query: `{duration>100ms}`,
			Check: func(t *testing.T, ast *TraceQLScript) {
				head := ast.Head.AttrSelector.Head
				if head == nil {
					t.Fatalf("expected a non-nil attribute selector head")
				}
				if head.Label != "duration" {
					t.Errorf("Label = %q, want %q", head.Label, "duration")
				}
				if head.Op != ">" {
					t.Errorf("Op = %q, want %q", head.Op, ">")
				}
				if head.Val.TimeVal != "100ms" {
					t.Errorf("Val.TimeVal = %q, want %q", head.Val.TimeVal, "100ms")
				}
			},
		},
		{
			Name:  "conjunction_inside_selector",
			Query: `{.a="x" && .b="y"}`,
			Check: func(t *testing.T, ast *TraceQLScript) {
				exp := ast.Head.AttrSelector
				if exp.AndOr != "&&" {
					t.Errorf("AndOr = %q, want %q", exp.AndOr, "&&")
				}
				if exp.Tail == nil || exp.Tail.Head == nil || exp.Tail.Head.Label != ".b" {
					t.Errorf("Tail head label = %+v, want .b", exp.Tail)
				}
			},
		},
		{
			Name:  "disjunction_inside_selector",
			Query: `{.a="x" || .b="y"}`,
			Check: func(t *testing.T, ast *TraceQLScript) {
				exp := ast.Head.AttrSelector
				if exp.AndOr != "||" {
					t.Errorf("AndOr = %q, want %q", exp.AndOr, "||")
				}
			},
		},
		{
			Name:  "pipeline_aggregator",
			Query: `{.a="x"} | count() > 2`,
			Check: func(t *testing.T, ast *TraceQLScript) {
				agg := ast.Head.Aggregator
				if agg == nil {
					t.Fatalf("expected a non-nil aggregator")
				}
				if agg.Fn != "count" {
					t.Errorf("Fn = %q, want %q", agg.Fn, "count")
				}
				if agg.Cmp != ">" {
					t.Errorf("Cmp = %q, want %q", agg.Cmp, ">")
				}
				if agg.Num != "2" {
					t.Errorf("Num = %q, want %q", agg.Num, "2")
				}
			},
		},
		{
			Name:  "script_level_and",
			Query: `{.a="x"} && {.b="y"}`,
			Check: func(t *testing.T, ast *TraceQLScript) {
				if ast.Op != "&&" {
					t.Errorf("Op = %q, want %q", ast.Op, "&&")
				}
				if ast.Tail == nil {
					t.Fatalf("expected a non-nil Tail")
				}
			},
		},
		{
			Name:  "script_level_or",
			Query: `{.a="x"} || {.b="y"}`,
			Check: func(t *testing.T, ast *TraceQLScript) {
				if ast.Op != "||" {
					t.Errorf("Op = %q, want %q", ast.Op, "||")
				}
			},
		},
		{
			Name:  "structural_descendant",
			Query: `{.a="x"} &>> {.b="y"}`,
			Check: func(t *testing.T, ast *TraceQLScript) {
				if ast.Op != "&>>" {
					t.Errorf("Op = %q, want %q", ast.Op, "&>>")
				}
			},
		},
		{
			Name:  "structural_sibling",
			Query: `{.a="x"} ~ {.b="y"}`,
			Check: func(t *testing.T, ast *TraceQLScript) {
				if ast.Op != "~" {
					t.Errorf("Op = %q, want %q", ast.Op, "~")
				}
			},
		},
		{
			Name:  "paren_wrapped",
			Query: `({.a="x"})`,
			Check: func(t *testing.T, ast *TraceQLScript) {
				if ast.ParenExpr == nil {
					t.Fatalf("expected a non-nil ParenExpr")
				}
			},
		},
		{
			// AttrSelectorExp's grammar offers a dedicated BoolLiteral
			// alternative for "true"/"false", but it is never actually
			// reached for a bare `{true}`: AttrSelector.Head is tried first
			// in the alternation, and "true" already matches its Label
			// field (Label_name matches any identifier, participle-keyword
			// or not), so parsing commits to the Head branch first. This
			// pin intentionally captures that real AST shape rather than
			// the grammar's aspirational BoolLiteral field.
			Name:  "bool_literal",
			Query: `{true}`,
			Check: func(t *testing.T, ast *TraceQLScript) {
				if ast.Head.AttrSelector.BoolLiteral != "" {
					t.Errorf("BoolLiteral = %q, want empty (Head branch wins)", ast.Head.AttrSelector.BoolLiteral)
				}
				head := ast.Head.AttrSelector.Head
				if head == nil || head.Label != "true" {
					t.Errorf("Head = %+v, want Label \"true\"", head)
				}
			},
		},

		{
			Name:  "resource_prefixed_regex_matcher",
			Query: `{ resource.service.name =~ "ship.*" }`,
			Check: func(t *testing.T, ast *TraceQLScript) {
				head := ast.Head.AttrSelector.Head
				if head == nil || head.Op != "=~" {
					t.Fatalf("expected a =~ matcher, got %+v", head)
				}
			},
		},
		{
			Name:  "mixed_attr_and_intrinsic_conjunction",
			Query: `{ resource.service.name = "checkout" && status = error }`,
			Check: func(t *testing.T, ast *TraceQLScript) {
				exp := ast.Head.AttrSelector
				if exp.AndOr != "&&" || exp.Tail == nil || exp.Tail.Head == nil {
					t.Fatalf("expected an &&-chained tail, got %+v", exp)
				}
				if exp.Tail.Head.Label != "status" {
					t.Errorf("expected the tail's label to be the intrinsic 'status', got %q", exp.Tail.Head.Label)
				}
			},
		},
		{
			Name:  "aggregator_with_attr_arg",
			Query: `{ status = ok } | avg(duration) > 0`,
			Check: func(t *testing.T, ast *TraceQLScript) {
				agg := ast.Head.Aggregator
				if agg == nil || agg.Fn != "avg" {
					t.Fatalf("expected an avg aggregator, got %+v", agg)
				}
				if agg.Attr != "duration" {
					t.Errorf("expected Attr=duration, got %q", agg.Attr)
				}
			},
		},
		{
			Name:  "metrics_pipeline_with_by_grouping",
			Query: `{ } | count_over_time() by (resource.service.name)`,
			Check: func(t *testing.T, ast *TraceQLScript) {
				mf := ast.Head.MetricsFn
				if mf == nil || mf.Fn != "count_over_time" {
					t.Fatalf("expected a count_over_time MetricsFn, got %+v", ast.Head)
				}
				if mf.By == nil {
					t.Errorf("expected a non-nil by(...) clause")
				}
			},
		},
		{
			Name:  "nil_comparison",
			Query: `{ kind != nil }`,
			Check: func(t *testing.T, ast *TraceQLScript) {
				head := ast.Head.AttrSelector.Head
				if head == nil || head.Op != "!=" {
					t.Fatalf("expected a != matcher, got %+v", head)
				}
				if head.Val.NilVal == "" {
					t.Errorf("expected Val.NilVal to be set, got %+v", head.Val)
				}
			},
		},
		{
			Name:  "attribute_to_attribute_comparison",
			Query: `{ span.child.index = resource.deployment.env }`,
			Check: func(t *testing.T, ast *TraceQLScript) {
				head := ast.Head.AttrSelector.Head
				if head == nil {
					t.Fatalf("expected a non-nil attribute selector head")
				}
				if head.Val.UnquotVal != "resource.deployment.env" {
					t.Errorf("expected the RHS to be the unquoted attribute path, got %+v", head.Val)
				}
			},
		},
		{
			Name:  "numeric_intrinsic_comparison",
			Query: `{ name > 3 }`,
			Check: func(t *testing.T, ast *TraceQLScript) {
				head := ast.Head.AttrSelector.Head
				if head == nil || head.Op != ">" {
					t.Fatalf("expected a > matcher, got %+v", head)
				}
				if head.Val.FVal != "3" {
					t.Errorf("expected Val.FVal=3, got %+v", head.Val)
				}
			},
		},

		// --- invalid queries: expected to fail to parse ---
		{
			Name:    "unclosed_brace",
			Query:   `{.a="x"`,
			WantErr: true,
		},
		{
			Name:    "unterminated_string",
			Query:   `{.a="x}`,
			WantErr: true,
		},
		{
			// Plain ">>" is not a valid script-level structural operator token in
			// this grammar — only "&>>" (Descendant) is. See lexer_rules v2.go
			// and TraceQLScript.Op's token alternation in model_v2.go.
			Name:    "bare_descendant_token_not_supported",
			Query:   `{.a="x"} >> {.b="y"}`,
			WantErr: true,
		},
		{
			// A single "&" is not a defined lexer token (only "&&", "&>>"
			// are) so this fails at the lexer, before grammar matching even
			// starts.
			Name:    "lone_ampersand_not_a_token",
			Query:   `{.a="x" & .b="y"}`,
			WantErr: true,
		},
		{
			Name:    "unclosed_aggregator_call",
			Query:   `{.a="x"} | count(`,
			WantErr: true,
		},
		{
			// Distinct failure class from "bare_descendant_token_not_supported"
			// (">>"): a single ">" isn't a script-level structural operator
			// either — only Descendant/NotDescendant/Ancestor/NotAncestor/
			// Sibling/And/Or are (see TraceQLScript.Op's alternation).
			Name:    "bare_gt_not_a_structural_operator",
			Query:   `{.a="x"} > {.b="y"}`,
			WantErr: true,
		},
		{
			// "!(" immediately preceding an expression fails at the lexer,
			// same failure layer as "lone_ampersand_not_a_token" but a
			// different offending token.
			Name:    "boolean_negation_not_supported",
			Query:   `{ !(kind = client) }`,
			WantErr: true,
		},
	})
}
