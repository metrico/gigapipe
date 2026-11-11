package traceql_ir

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func cmp(attr, op string, kind ValueKind, text string) *BinaryOp {
	return &BinaryOp{Op: op, LHS: &Attribute{Name: attr}, RHS: &Static{Kind: kind, Text: text}}
}

func strCmp(attr, op, text string) *BinaryOp { return cmp(attr, op, ValueString, text) }

func bin(op string, lhs, rhs FieldExpr) *BinaryOp { return &BinaryOp{Op: op, LHS: lhs, RHS: rhs} }

func sel(f FieldExpr) *Selector { return &Selector{Filter: f} }

func a() *Selector { return sel(strCmp(".a", "=", `"a"`)) }
func b() *Selector { return sel(strCmp(".b", "=", `"b"`)) }
func c() *Selector { return sel(strCmp(".c", "=", `"c"`)) }
func d() *Selector { return sel(strCmp(".d", "=", `"d"`)) }

func op(o SpansetOperator, lhs, rhs SpansetExpr) *SpansetOp {
	return &SpansetOp{Op: o, LHS: lhs, RHS: rhs}
}

func pipeline(e SpansetExpr) Pipeline { return Pipeline{Elements: []Element{e}} }

func mustParse(t *testing.T, q string) *Query {
	t.Helper()
	got, err := Parse(q)
	require.NoError(t, err)
	return got
}

func TestLowerFilters(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  FieldExpr
	}{
		{`{}`, nil},
		{`{.a = "b"}`, strCmp(".a", "=", `"b"`)},
		{"{.a = `b`}", strCmp(".a", "=", "`b`")},
		{`{span.x != 200}`, cmp("span.x", "!=", ValueNumber, "200")},
		{`{.a > -3}`, cmp(".a", ">", ValueNumber, "-3")},
		{`{.a > - -3}`, cmp(".a", ">", ValueNumber, "3")},
		{`{.a < 1 . 5}`, cmp(".a", "<", ValueNumber, "1.5")},
		{`{duration > 1.5ms}`, cmp("duration", ">", ValueDuration, "1.5ms")},
		{`{duration > -1ms}`, cmp("duration", ">", ValueDuration, "-1ms")},
		{`{.a = true}`, cmp(".a", "=", ValueBool, "true")},
		{`{status = error}`, cmp("status", "=", ValueIdent, "error")},
		{`{span.foo = bar}`, cmp("span.foo", "=", ValueIdent, "bar")},
		{`{.a}`, &Attribute{Name: ".a"}},
		{`{ false }`, &Static{Kind: ValueBool, Text: "false"}},
		{`{.a = .b}`, bin("=", &Attribute{Name: ".a"}, &Attribute{Name: ".b"})},
		{`{.a-.b = 1}`, cmp(".a-.b", "=", ValueNumber, "1")},
		{`{.a - .b = 1}`, bin("=", bin("-", &Attribute{Name: ".a"}, &Attribute{Name: ".b"}), &Static{Kind: ValueNumber, Text: "1"})},
		{`{.a = nil}`, &NilTest{Expr: &Attribute{Name: ".a"}}},
		{`{nil != span.a}`, &NilTest{Expr: &Attribute{Name: "span.a"}, Exists: true}},
		{`{nil = nil}`, &Static{Kind: ValueBool, Text: "false"}},
		{`{!(.a = 1)}`, &UnaryOp{Op: "!", Expr: cmp(".a", "=", ValueNumber, "1")}},
		{`{name = "x" && span:duration > 1s && rootServiceName = "y"}`,
			bin("&&", bin("&&", strCmp("name", "=", `"x"`), cmp("span:duration", ">", ValueDuration, "1s")),
				strCmp("rootServiceName", "=", `"y"`))},
		{`{(.a = 1 || .b = 2) && .c = 3}`,
			bin("&&", bin("||", cmp(".a", "=", ValueNumber, "1"), cmp(".b", "=", ValueNumber, "2")),
				cmp(".c", "=", ValueNumber, "3"))},
	} {
		t.Run(tc.query, func(t *testing.T) {
			require.Equal(t, &Query{Pipeline: pipeline(sel(tc.want))}, mustParse(t, tc.query))
		})
	}
}

// Field expressions group by precedence: `!` binds tighter than the
// comparisons and looser than `* / %`; unary `-` binds like binary `-`.
func TestLowerFieldPrecedence(t *testing.T) {
	for query, want := range map[string]string{
		`{.x || .y && .z}`:           `(.x || .y) && .z`,
		`{.x && .y || .z}`:           `(.x && .y) || .z`,
		`{.a + .b * .c > 1}`:         `(.a + (.b * .c)) > 1`,
		`{.a - .b - .c > 1}`:         `((.a - .b) - .c) > 1`,
		`{.a % .b * .c > 1}`:         `((.a % .b) * .c) > 1`,
		`{.a ^ .b ^ .c > 1}`:         `(.a ^ (.b ^ .c)) > 1`,
		`{.a * .b ^ .c > 1}`:         `(.a * (.b ^ .c)) > 1`,
		`{.a / .b + .c ^ 2 > 1}`:     `((.a / .b) + (.c ^ 2)) > 1`,
		`{.a = 1 = true}`:            `(.a = 1) = true`,
		`{-.a + 1 > 0}`:              `((-.a) + 1) > 0`,
		`{-.a * 2 > 0}`:              `(-(.a * 2)) > 0`,
		`{-2 ^ 2 = -4}`:              `(-(2 ^ 2)) = -4`,
		`{- - .a > 0}`:               `(-(-.a)) > 0`,
		`{.a * -.b + 1 > 0}`:         `((.a * (-.b)) + 1) > 0`,
		`{.a * -.b * 3 > 0}`:         `(.a * (-(.b * 3))) > 0`,
		`{!.a = 1}`:                  `(!.a) = 1`,
		`{!.a * 2}`:                  `!(.a * 2)`,
		`{!.x && .y}`:                `(!.x) && .y`,
		`{! ! .x}`:                   `!(!.x)`,
		`{!(.a = 1) || .b = 2}`:      `(!(.a = 1)) || (.b = 2)`,
		`{.a + 1 = nil}`:             `(.a + 1) = nil`,
		`{nil = .a + 1}`:             `(.a + 1) = nil`,
		`{.a = nil && .b != nil}`:    `(.a = nil) && (.b != nil)`,
		`{.a =~ "x" || .b !~ "y"}`:   "(.a =~ \"x\") || (.b !~ \"y\")",
		`{.a <= 1 || .a >= 2}`:       `(.a <= 1) || (.a >= 2)`,
		`{(.a + .b) * .c < 1}`:       `((.a + .b) * .c) < 1`,
		`{.a-.b + .c-.d > 1}`:        `(.a-.b + .c-.d) > 1`,
		`{duration * 2 > 1s}`:        `(duration * 2) > 1s`,
		`{.a / 2 = 2 || .a % 2 = 1}`: `((.a / 2) = 2) || ((.a % 2) = 1)`,
	} {
		t.Run(query, func(t *testing.T) {
			require.Equal(t, want, mustParse(t, query).Pipeline.Elements[0].(*Selector).Filter.String())
		})
	}
}

func TestLowerRejectsNil(t *testing.T) {
	for _, q := range []string{`{nil}`, `{.a > nil}`, `{.a + nil = 1}`, `{-nil = 1}`, `{!nil}`, `{.a =~ nil}`,
		`{} | avg(nil) > 1`, `{(nil) = .a}`} {
		_, err := Parse(q)
		require.EqualError(t, err, errUnexpectedNil.Error(), q)
	}
}

func stages(elements ...Element) Pipeline { return Pipeline{Elements: elements} }

func count(op, value string) *ScalarFilter {
	return &ScalarFilter{Op: op, LHS: &Aggregate{Fn: "count"}, RHS: &Static{Kind: ValueNumber, Text: value}}
}

func TestLowerScalarFilters(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  Pipeline
	}{
		{`{.a = "a"} | avg(duration) >= 1.5ms`, stages(a(), &ScalarFilter{Op: ">=",
			LHS: &Aggregate{Fn: "avg", Arg: &Attribute{Name: "duration"}}, RHS: &Static{Kind: ValueDuration, Text: "1.5ms"}})},
		{`count() > 2`, stages(count(">", "2"))},
		{`{.a = "a"} >> {.b = "b"} | count() > 2 | max(span.x) < -3`, stages(op(OpDescendant, a(), b()), count(">", "2"),
			&ScalarFilter{Op: "<", LHS: &Aggregate{Fn: "max", Arg: &Attribute{Name: "span.x"}}, RHS: &Static{Kind: ValueNumber, Text: "-3"}})},
		{`{} | 2 < count()`, stages(sel(nil), &ScalarFilter{Op: "<", LHS: &Static{Kind: ValueNumber, Text: "2"}, RHS: &Aggregate{Fn: "count"}})},
		// Arithmetic: `^` binds tightest and is right-associative, then `* / %`, then `+ -`.
		{`{} | sum(.a) - 1 * 2 ^ 3 ^ 4 > (count() + 1) % 2`, stages(sel(nil), &ScalarFilter{Op: ">",
			LHS: &ScalarOp{Op: "-", LHS: &Aggregate{Fn: "sum", Arg: &Attribute{Name: ".a"}}, RHS: &ScalarOp{Op: "*",
				LHS: &Static{Kind: ValueNumber, Text: "1"},
				RHS: &ScalarOp{Op: "^", LHS: &Static{Kind: ValueNumber, Text: "2"},
					RHS: &ScalarOp{Op: "^", LHS: &Static{Kind: ValueNumber, Text: "3"}, RHS: &Static{Kind: ValueNumber, Text: "4"}}}}},
			RHS: &ScalarOp{Op: "%", LHS: &ScalarOp{Op: "+", LHS: &Aggregate{Fn: "count"}, RHS: &Static{Kind: ValueNumber, Text: "1"}},
				RHS: &Static{Kind: ValueNumber, Text: "2"}}})},
	} {
		t.Run(tc.query, func(t *testing.T) {
			require.Equal(t, &Query{Pipeline: tc.want}, mustParse(t, tc.query))
		})
	}
}

func TestLowerPipelines(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  Pipeline
	}{
		{`{.a = "a"} | {.b = "b"}`, stages(a(), b())},
		{`({.a = "a"} | count() > 2) >> {.b = "b"}`,
			stages(op(OpDescendant, &Pipeline{Elements: []Element{a(), count(">", "2")}}, b()))},
		{`{.b = "b"} || ({.a = "a"} && {.c = "c"} | count() > 2) | count() > 1`,
			stages(op(OpOr, b(), &Pipeline{Elements: []Element{op(OpAnd, a(), c()), count(">", "2")}}), count(">", "1"))},
		{`({.a = "a"})`, stages(&Group{Expr: a()})},
	} {
		t.Run(tc.query, func(t *testing.T) {
			require.Equal(t, &Query{Pipeline: tc.want}, mustParse(t, tc.query))
		})
	}
}

func TestAggregateCanonicalForm(t *testing.T) {
	for query, want := range map[string]string{
		`{} | count() > 1`:              "count()",
		`{} | avg(duration) > 1`:        "avg(duration)",
		`{} | sum(span.a) > 1`:          "sum(span.a)",
		`{} | min(.a) > 1`:              "min(.a)",
		`{} | max(span:duration) > 1ms`: "max(span:duration)",
		`{} | avg(spanN) > 1`:           "avg(spanN)",
		`{} | avg(.a + .b) > 1`:         "avg(.a + .b)",
		`{} | sum(.a*(.b - 1)) > 1`:     "sum(.a * (.b - 1))",
		`{} | sum(.a*(.b-1)) > 1`:       "sum(.a * .b-1)",
		`{} | max(-duration) > 1`:       "max(-duration)",
	} {
		f := mustParse(t, query).Pipeline.Elements[1].(*ScalarFilter)
		require.Equal(t, want, f.LHS.String(), query)
	}
}

func TestLowerSpansetOperators(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  SpansetExpr
	}{
		{`{.a = "a"} && {.b = "b"}`, op(OpAnd, a(), b())},
		{`{.a = "a"} || {.b = "b"}`, op(OpOr, a(), b())},
		{`{.a = "a"} >> {.b = "b"}`, op(OpDescendant, a(), b())},
		{`{.a = "a"} << {.b = "b"}`, op(OpAncestor, a(), b())},
		{`{.a = "a"} !>> {.b = "b"}`, op(OpNotDescendant, a(), b())},
		{`{.a = "a"} !<< {.b = "b"}`, op(OpNotAncestor, a(), b())},
		{`{.a = "a"} &>> {.b = "b"}`, op(OpUnionDescendant, a(), b())},
		{`{.a = "a"} &<< {.b = "b"}`, op(OpUnionAncestor, a(), b())},
		{`{.a = "a"} ~ {.b = "b"}`, op(OpSibling, a(), b())},
		{`{.a = "a"} > {.b = "b"}`, op(OpChild, a(), b())},
		{`{.a = "a"} < {.b = "b"}`, op(OpParent, a(), b())},
		{`{.a = "a"} !> {.b = "b"}`, op(OpNotChild, a(), b())},
		{`{.a = "a"} !< {.b = "b"}`, op(OpNotParent, a(), b())},
		{`{.a = "a"} !~ {.b = "b"}`, op(OpNotSibling, a(), b())},
		{`{.a = "a"} &> {.b = "b"}`, op(OpUnionChild, a(), b())},
		{`{.a = "a"} &< {.b = "b"}`, op(OpUnionParent, a(), b())},
		{`{.a = "a"} &~ {.b = "b"}`, op(OpUnionSibling, a(), b())},
		// Structural operators bind tighter than `&&` = `||`; all left-associative.
		{`{.a = "a"} && {.b = "b"} && {.c = "c"}`, op(OpAnd, op(OpAnd, a(), b()), c())},
		{`{.a = "a"} || {.b = "b"} || {.c = "c"}`, op(OpOr, op(OpOr, a(), b()), c())},
		{`{.a = "a"} && {.b = "b"} || {.c = "c"}`, op(OpOr, op(OpAnd, a(), b()), c())},
		{`{.a = "a"} || {.b = "b"} && {.c = "c"}`, op(OpAnd, op(OpOr, a(), b()), c())},
		{`{.a = "a"} ~ {.b = "b"} ~ {.c = "c"}`, op("~", op("~", a(), b()), c())},
		{`{.a = "a"} ~ {.b = "b"} && {.c = "c"} || {.d = "d"}`,
			op(OpOr, op(OpAnd, op("~", a(), b()), c()), d())},
		{`{.a = "a"} || {.b = "b"} &>> {.c = "c"} || {.d = "d"}`,
			op(OpOr, op(OpOr, a(), op(OpUnionDescendant, b(), c())), d())},
		{`{.a = "a"} && {.b = "b"} &>> {.c = "c"} !>> {.d = "d"}`,
			op(OpAnd, a(), op(OpNotDescendant, op(OpUnionDescendant, b(), c()), d()))},
		{`{.a = "a"} >> {.b = "b"} < {.c = "c"} << {.d = "d"}`,
			op(OpAncestor, op(OpParent, op(OpDescendant, a(), b()), c()), d())},
		{`{.a = "a"} << {.b = "b"} || {.c = "c"} >> {.d = "d"}`,
			op(OpOr, op(OpAncestor, a(), b()), op(OpDescendant, c(), d()))},
		{`{.a = "a"} > {.b = "b"} > {.c = "c"}`, op(OpChild, op(OpChild, a(), b()), c())},
		{`{.a = "a"} > ({.b = "b"} > {.c = "c"})`, op(OpChild, a(), &Group{Expr: op(OpChild, b(), c())})},
		{`{.a = "a"} !~ {.b = "b"} &< {.c = "c"} &~ {.d = "d"}`,
			op(OpUnionSibling, op(OpUnionParent, op(OpNotSibling, a(), b()), c()), d())},
		{`{.a = "a"} || {.b = "b"} > {.c = "c"} && {.d = "d"}`,
			op(OpAnd, op(OpOr, a(), op(OpChild, b(), c())), d())},
		{`{.a = "a"} < {.b = "b"} || {.c = "c"} !< {.d = "d"}`,
			op(OpOr, op(OpParent, a(), b()), op(OpNotParent, c(), d()))},
		{`({.a = "a"})`, &Group{Expr: a()}},
		{`(({.a = "a"}))`, &Group{Expr: &Group{Expr: a()}}},
		{`({.a = "a"} || {.b = "b"}) && {.c = "c"}`,
			op(OpAnd, &Group{Expr: op(OpOr, a(), b())}, c())},
		{`{.c = "c"} && ({.a = "a"} || {.b = "b"})`,
			op(OpAnd, c(), &Group{Expr: op(OpOr, a(), b())})},
	} {
		t.Run(tc.query, func(t *testing.T) {
			require.Equal(t, &Query{Pipeline: pipeline(tc.want)}, mustParse(t, tc.query))
		})
	}
}

func TestLowerMetrics(t *testing.T) {
	p := 0.9
	from, to := int64(1), int64(2)
	for _, tc := range []struct {
		query string
		want  *Query
	}{
		{`{.a = "a"} | rate()`, &Query{Pipeline: pipeline(a()), Metrics: &MetricsFn{Fn: "rate"}}},
		{`{.a = "a"} | count_over_time() by (span.x, name)`, &Query{Pipeline: pipeline(a()),
			Metrics: &MetricsFn{Fn: "count_over_time", By: []string{"span.x", "name"}}}},
		{`{.a = "a"} | quantile_over_time(duration, 0.9)`, &Query{Pipeline: pipeline(a()),
			Metrics: &MetricsFn{Fn: "quantile_over_time", Attr: "duration", Percentile: &p}}},
		{`{.a = "a"} | compare({.b = "b"}, 10, 1, 2)`, &Query{Pipeline: pipeline(a()),
			Metrics: &MetricsFn{Fn: "compare", Compare: &CompareArgs{
				Filter: strCmp(".b", "=", `"b"`), Count: 10, BaselineFrom: &from, BaselineTo: &to}}}},
		{`{.a = "a"} | compare({})`, &Query{Pipeline: pipeline(a()),
			Metrics: &MetricsFn{Fn: "compare", Compare: &CompareArgs{}}}},
		{`({.a = "a"} &>> {.b = "b"}) | rate()`, &Query{
			Pipeline: pipeline(&Group{Expr: op(OpUnionDescendant, a(), b())}), Metrics: &MetricsFn{Fn: "rate"}}},
		{`count() > 1 | rate()`, &Query{Pipeline: stages(count(">", "1")), Metrics: &MetricsFn{Fn: "rate"}}},
		{`{.a = "a"} >> {.b = "b"} | count() > 1 | rate()`, &Query{
			Pipeline: stages(op(OpDescendant, a(), b()), count(">", "1")), Metrics: &MetricsFn{Fn: "rate"}}},
		// `|` binds looser than spanset operators.
		{`{.a = "a"} && {.b = "b"} | rate()`, &Query{Pipeline: pipeline(op(OpAnd, a(), b())), Metrics: &MetricsFn{Fn: "rate"}}},
		{`{.a = "a"} >> {.b = "b"} | count_over_time() by (name) with(sample=0.1) | topk(2)`, &Query{
			Pipeline:    pipeline(op(OpDescendant, a(), b())),
			Metrics:     &MetricsFn{Fn: "count_over_time", By: []string{"name"}},
			Hints:       []Hint{{Key: "sample", Value: "0.1"}},
			SecondStage: &SecondStage{Fn: "topk", Count: 2},
		}},
		{`{.a = "a"} > ({.b = "b"}) | rate()`, &Query{
			Pipeline: pipeline(op(OpChild, a(), &Group{Expr: b()})), Metrics: &MetricsFn{Fn: "rate"}}},
		{`{.a = "a"} | rate() with(sample=0.1, exemplars=false) | topk(5)`, &Query{
			Pipeline:    pipeline(a()),
			Metrics:     &MetricsFn{Fn: "rate"},
			Hints:       []Hint{{Key: "sample", Value: "0.1"}, {Key: "exemplars", Value: "false"}},
			SecondStage: &SecondStage{Fn: "topk", Count: 5},
		}},
		{`{.a = "a"} | rate() | select(span.a, duration) with(sample=0.5)`, &Query{
			Pipeline: pipeline(a()),
			Metrics:  &MetricsFn{Fn: "rate"},
			SecondStage: &SecondStage{Fn: "select", Labels: []string{"span.a", "duration"},
				Hints: []Hint{{Key: "sample", Value: "0.5"}}},
		}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			require.Equal(t, tc.want, mustParse(t, tc.query))
		})
	}
}

func TestStaticUnquote(t *testing.T) {
	for raw, want := range map[string]string{`"a\"b"`: `a"b`, "`a\\b`": `a\b`} {
		got, err := (&Static{Kind: ValueString, Text: raw}).Unquote()
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

func TestParseError(t *testing.T) {
	_, err := Parse(`{`)
	require.Error(t, err)
}

func TestHasStructural(t *testing.T) {
	for query, want := range map[string]bool{
		`{.a = "a"}`:                                false,
		`{.a = "a"} && {.b = "b"} || {.c = "c"}`:    false,
		`{.a = "a"} > {.b = "b"}`:                   true,
		`{.a = "a"} !~ {.b = "b"}`:                  true,
		`{.a = "a"} >> {.b = "b"}`:                  true,
		`{.a = "a"} && ({.b = "b"} &<< {.c = "c"})`: true,
		`({.a = "a"} !<< {.b = "b"}) | rate()`:      true,
		`({.a = "a"} > {.b = "b"} | count() > 1)`:   true,
		`({.a = "a"} | count() > 1) && {.b = "b"}`:  false,
		`count() > 1`:                               false,
	} {
		require.Equal(t, want, mustParse(t, query).HasStructural(), query)
	}
}
