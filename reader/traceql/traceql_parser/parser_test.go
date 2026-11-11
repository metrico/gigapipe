package traceql_parser

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func name(n string) FieldOperand { return FieldOperand{Atom: FieldAtom{Name: n}} }

func str(s string) FieldOperand { return FieldOperand{Atom: FieldAtom{Str: &QuotedString{Str: s}}} }

func num(n string) FieldOperand { return FieldOperand{Atom: FieldAtom{Number: &NumberLiteral{Num: n}}} }

func dur(n, unit string) FieldOperand {
	return FieldOperand{Atom: FieldAtom{Number: &NumberLiteral{Num: n, Unit: unit}}}
}

func unary(ops string, o FieldOperand) FieldOperand {
	o.Unary = strings.Split(ops, "")
	return o
}

func parenField(e *FieldExpr) FieldOperand { return FieldOperand{Atom: FieldAtom{Paren: e}} }

// field is head followed by alternating operators and operands.
func field(head FieldOperand, rest ...any) *FieldExpr {
	res := &FieldExpr{Head: head}
	for i := 0; i < len(rest); i += 2 {
		res.Ops = append(res.Ops, FieldBinary{Op: rest[i].(string), Operand: rest[i+1].(FieldOperand)})
	}
	return res
}

func term(label, op string, val FieldOperand) *FieldExpr { return field(name(label), op, val) }

// join appends op and tail to head, as written.
func join(head *FieldExpr, op string, tail *FieldExpr) *FieldExpr {
	res := *head
	res.Ops = append(append(append([]FieldBinary{}, head.Ops...), FieldBinary{Op: op, Operand: tail.Head}), tail.Ops...)
	return &res
}

func sel(exp *FieldExpr) Operand { return Operand{Selector: Selector{Filter: exp}} }

func expr(head Operand, ops ...SpansetOperation) *SpansetExpr {
	return &SpansetExpr{Head: head, Ops: ops}
}

func then(op string, operand Operand) SpansetOperation {
	return SpansetOperation{Op: op, Operand: operand}
}

func paren(head *SpansetExpr, stages ...PipelineElement) Operand {
	return Operand{Paren: &ParenPipeline{Head: *head, Stages: stages}}
}

func spanset(e *SpansetExpr) PipelineElement { return PipelineElement{Spanset: e} }

// script is a query of one spanset expression followed by stages.
func script(e *SpansetExpr, stages ...PipelineElement) *TraceQLScript {
	return &TraceQLScript{Pipeline: Pipeline{Head: spanset(e), Stages: stages}}
}

func scalar(atom ScalarAtom) ScalarExpr {
	return ScalarExpr{Head: ScalarTerm{Head: ScalarFactor{Base: atom}}}
}

func agg(fn, attr string) ScalarExpr {
	a := &Aggregate{Fn: fn}
	if attr != "" {
		a.Arg = field(name(attr))
	}
	return scalar(ScalarAtom{Aggregate: a})
}

func lit(num, unit string) ScalarExpr {
	return scalar(ScalarAtom{Literal: &ScalarLiteral{Num: num, Unit: unit}})
}

func filter(lhs ScalarExpr, op string, rhs ScalarExpr) PipelineElement {
	return PipelineElement{ScalarFilter: &ScalarFilter{LHS: lhs, Op: op, RHS: rhs}}
}

func groupBy(e *FieldExpr) PipelineElement { return PipelineElement{GroupBy: &GroupBy{Expr: *e}} }

func coalesce() PipelineElement { return PipelineElement{Coalesce: &Coalesce{Fn: "coalesce"}} }

func selectOf(fields ...*FieldExpr) PipelineElement {
	res := &Select{}
	for _, f := range fields {
		res.Fields = append(res.Fields, *f)
	}
	return PipelineElement{Select: res}
}

func f64(f float64) *float64 { return &f }

func i64(i int64) *int64 { return &i }

func TestParseSelectors(t *testing.T) {
	for _, c := range []struct {
		query string
		want  *FieldExpr
	}{
		{`{}`, nil},
		{`{.a = "b"}`, term(".a", "=", str(`"b"`))},
		{"{.a = `b`}", term(".a", "=", str("`b`"))},
		{`{span.http.status_code != 200}`, term("span.http.status_code", "!=", num("200"))},
		{`{resource.service.name =~ "api-.*"}`, term("resource.service.name", "=~", str(`"api-.*"`))},
		{`{.a !~ "x"}`, term(".a", "!~", str(`"x"`))},
		{`{.a > -3}`, term(".a", ">", unary("-", num("3")))},
		{`{.a >= 1.5}`, term(".a", ">=", num("1.5"))},
		{`{.a < 1 . 5}`, term(".a", "<", num("1.5"))},
		{`{.a <= 0}`, term(".a", "<=", num("0"))},
		{`{duration > 1.5ms}`, term("duration", ">", dur("1.5", "ms"))},
		{`{span:duration < 2s}`, term("span:duration", "<", dur("2", "s"))},
		{`{.a = nil}`, term(".a", "=", name("nil"))},
		{`{.a != nil}`, term(".a", "!=", name("nil"))},
		{`{.a = true}`, term(".a", "=", name("true"))},
		{`{status = error}`, term("status", "=", name("error"))},
		{`{kind = server}`, term("kind", "=", name("server"))},
		{`{.a-b.c = "x"}`, term(".a-b.c", "=", str(`"x"`))},
		{`{.a}`, field(name(".a"))},
		{`{.a = .b}`, term(".a", "=", name(".b"))},
		{`{ true }`, field(name("true"))},
		{`{.a = "b" && .c = "d"}`,
			join(term(".a", "=", str(`"b"`)), "&&", term(".c", "=", str(`"d"`)))},
		{`{.a = "b" || .c = "d" && .e = "f"}`,
			join(join(term(".a", "=", str(`"b"`)), "||", term(".c", "=", str(`"d"`))), "&&", term(".e", "=", str(`"f"`)))},
		{`{(.a = 1 || .b = 2) && .c = 3}`,
			field(parenField(join(term(".a", "=", num("1")), "||", term(".b", "=", num("2")))), "&&", name(".c"), "=", num("3"))},
	} {
		t.Run(c.query, func(t *testing.T) {
			got, err := Parse(c.query)
			require.NoError(t, err)
			require.Equal(t, script(expr(sel(c.want))), got)
		})
	}
}

// Field expressions parse as written; lowering groups them by precedence.
func TestParseFieldExpressions(t *testing.T) {
	for _, c := range []struct {
		query string
		want  *FieldExpr
	}{
		// A hyphen inside an unquoted name is part of it; binary - needs whitespace.
		{`{.a-.b > 3}`, term(".a-.b", ">", num("3"))},
		{`{.a - .b > 3}`, field(name(".a"), "-", name(".b"), ">", num("3"))},
		{`{.a -.b > 3}`, field(name(".a"), "-", name(".b"), ">", num("3"))},
		{`{.a-1 = 2}`, term(".a-1", "=", num("2"))},
		{`{.a + .b * .c / .d % .e ^ .f = 1}`,
			field(name(".a"), "+", name(".b"), "*", name(".c"), "/", name(".d"), "%", name(".e"), "^", name(".f"), "=", num("1"))},
		{`{.a+.b*2>3}`, field(name(".a"), "+", name(".b"), "*", num("2"), ">", num("3"))},
		{`{-.a * 2 = -4}`, field(unary("-", name(".a")), "*", num("2"), "=", unary("-", num("4")))},
		{`{1 - -2 = 3}`, field(num("1"), "-", unary("-", num("2")), "=", num("3"))},
		{`{!(.a = 1)}`, field(unary("!", parenField(term(".a", "=", num("1")))))},
		{`{!.a}`, field(unary("!", name(".a")))},
		{`{! ! .a}`, field(unary("!!", name(".a")))},
		{`{!-.a > 1}`, field(unary("!-", name(".a")), ">", num("1"))},
		{`{nil != .a}`, field(name("nil"), "!=", name(".a"))},
		{`{.a = .b || .c <= .d}`, field(name(".a"), "=", name(".b"), "||", name(".c"), "<=", name(".d"))},
		{`{(.a + .b) * 2 >= 1ms}`, field(parenField(field(name(".a"), "+", name(".b"))), "*", num("2"), ">=", dur("1", "ms"))},
	} {
		t.Run(c.query, func(t *testing.T) {
			got, err := Parse(c.query)
			require.NoError(t, err)
			require.Equal(t, script(expr(sel(c.want))), got)
		})
	}

	t.Run("aggregate argument", func(t *testing.T) {
		got, err := Parse(`{} | avg(.a + -.b) > 2`)
		require.NoError(t, err)
		arg := field(name(".a"), "+", unary("-", name(".b")))
		require.Equal(t, script(expr(sel(nil)), filter(scalar(ScalarAtom{Aggregate: &Aggregate{Fn: "avg", Arg: arg}}), ">", lit("2", ""))), got)
	})

	for _, q := range []string{`{.a = }`, `{.a + }`, `{!}`, `{.a = 1 !}`, `{.a ** 2}`, `{.a = ()}`} {
		t.Run("rejects "+q, func(t *testing.T) {
			_, err := Parse(q)
			require.Error(t, err)
		})
	}
}

func TestParseSpansetOperators(t *testing.T) {
	a := func() Operand { return sel(term(".a", "=", str(`"b"`))) }
	c := func() Operand { return sel(term(".c", "=", str(`"d"`))) }
	for _, op := range []string{
		"&&", "||",
		">", "<", "~", "!>", "!<", "!~", "&>", "&<", "&~",
		">>", "<<", "!>>", "!<<", "&>>", "&<<",
	} {
		t.Run(op, func(t *testing.T) {
			want := script(expr(a(), then(op, c())))
			got, err := Parse(`{.a = "b"} ` + op + ` {.c = "d"}`)
			require.NoError(t, err)
			require.Equal(t, want, got)
			got, err = Parse(`{.a = "b"}` + op + `{.c = "d"}`)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}

	t.Run("chain in written order", func(t *testing.T) {
		got, err := Parse(`{.a = "b"} && {.c = "d"} || {}`)
		require.NoError(t, err)
		require.Equal(t, script(expr(a(), then("&&", c()), then("||", sel(nil)))), got)
	})

	t.Run("parenthesised expression", func(t *testing.T) {
		got, err := Parse(`({.a = "b"} &>> {.c = "d"}) || {}`)
		require.NoError(t, err)
		require.Equal(t, script(expr(paren(expr(a(), then("&>>", c()))), then("||", sel(nil)))), got)
	})

	// Inside {…} >, < and !~ compare; between spansets they are structural.
	for _, c := range []struct {
		query string
		want  *TraceQLScript
	}{
		{`{ .x > 3 } > { .y !~ "a.*" }`, script(expr(
			sel(term(".x", ">", num("3"))), then(">", sel(term(".y", "!~", str(`"a.*"`))))))},
		{`{.x<3}<{.y<4}`, script(expr(
			sel(term(".x", "<", num("3"))), then("<", sel(term(".y", "<", num("4"))))))},
		{`{ .x !~ "a" } !~ { .y =~ "b" }`, script(expr(
			sel(term(".x", "!~", str(`"a"`))), then("!~", sel(term(".y", "=~", str(`"b"`))))))},
		{`{ .x >= 1 } &> { .y <= 2 }`, script(expr(
			sel(term(".x", ">=", num("1"))), then("&>", sel(term(".y", "<=", num("2"))))))},
		{`{ .x = "<<&" } > {}`, script(expr(sel(term(".x", "=", str(`"<<&"`))), then(">", sel(nil))))},
	} {
		t.Run(c.query, func(t *testing.T) {
			got, err := Parse(c.query)
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}

	for _, q := range []string{
		// Split spellings.
		`{.a = "b"} ! > {.c = "d"}`,
		`{.a = "b"} ! < {.c = "d"}`,
		`{.a = "b"} ! ~ {.c = "d"}`,
		`{.a = "b"} & > {.c = "d"}`,
		`{.a = "b"} & < {.c = "d"}`,
		`{.a = "b"} & ~ {.c = "d"}`,
		`{.a = "b"} > > {.c = "d"}`,
		`{.a = "b"} < < {.c = "d"}`,
		`{.a = "b"} & >> {.c = "d"}`,
		`{.a = "b"} ! >> {.c = "d"}`,
		`{.a = "b"} & << {.c = "d"}`,
		`{.a = "b"} ! << {.c = "d"}`,
		`{.a = "b"} > >> {.c = "d"}`,
		`{.a = "b"} << < {.c = "d"}`,
		`{.a >> 1}`,
		// Structural operators inside a selector.
		`{.a > {.c = "d"}}`,
		`{.a &> 1}`,
		// A dangling operator.
		`{.a = "b"} &&`,
		`{.a = "b"} >> | count() > 1`,
	} {
		t.Run("rejects "+q, func(t *testing.T) {
			_, err := Parse(q)
			require.Error(t, err)
		})
	}

	for op, spelling := range map[string]string{"<<&": "&<<", "<<~": "!<<"} {
		t.Run("rejects misspelt "+op, func(t *testing.T) {
			_, err := Parse(`{.a = "b"} ` + op + ` {.c = "d"}`)
			require.EqualError(t, err, op+" is not a TraceQL operator, use "+spelling)
		})
	}
}

func TestParseScalarFilters(t *testing.T) {
	a := term(".a", "=", str(`"b"`))
	for _, c := range []struct {
		query string
		want  *TraceQLScript
	}{
		{`{.a = "b"} | count() > 2`, script(expr(sel(a)), filter(agg("count", ""), ">", lit("2", "")))},
		{`{} | count() = 3.5`, script(expr(sel(nil)), filter(agg("count", ""), "=", lit("3.5", "")))},
		{`{} | avg(duration) >= 1.5ms`, script(expr(sel(nil)), filter(agg("avg", "duration"), ">=", lit("1.5", "ms")))},
		{`{} | max(span.x) < -3`, script(expr(sel(nil)), filter(agg("max", "span.x"), "<", lit("-3", "")))},
		{`{} | min(.x) != 0`, script(expr(sel(nil)), filter(agg("min", ".x"), "!=", lit("0", "")))},
		{`{} | sum(resource.x) <= 10s`, script(expr(sel(nil)), filter(agg("sum", "resource.x"), "<=", lit("10", "s")))},
		{`{} | avg(span:duration) > 1 . 5`, script(expr(sel(nil)), filter(agg("avg", "span:duration"), ">", lit("1.5", "")))},
		// A scalar filter applies to the whole expression before it.
		{`{.a = "b"} && {} | count() > 1`, script(expr(sel(a), then("&&", sel(nil))),
			filter(agg("count", ""), ">", lit("1", "")))},
		{`{.a = "b"} >> {} | count() > 1 | avg(duration) > 2ms`, script(expr(sel(a), then(">>", sel(nil))),
			filter(agg("count", ""), ">", lit("1", "")), filter(agg("avg", "duration"), ">", lit("2", "ms")))},
		// Forms that parse and fail validation.
		{`{} | 2 < count()`, script(expr(sel(nil)), filter(lit("2", ""), "<", agg("count", "")))},
		{`{} | count() > count()`, script(expr(sel(nil)), filter(agg("count", ""), ">", agg("count", "")))},
		{`{} | sum(.a) + 1 > 0`, script(expr(sel(nil)), filter(ScalarExpr{
			Head: ScalarTerm{Head: ScalarFactor{Base: ScalarAtom{Aggregate: &Aggregate{Fn: "sum", Arg: field(name(".a"))}}}},
			Ops:  []ScalarSumOp{{Op: "+", Term: ScalarTerm{Head: ScalarFactor{Base: ScalarAtom{Literal: &ScalarLiteral{Num: "1"}}}}}},
		}, ">", lit("0", "")))},
	} {
		t.Run(c.query, func(t *testing.T) {
			got, err := Parse(c.query)
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}

	t.Run("a bare scalar filter", func(t *testing.T) {
		got, err := Parse(`count() > 2`)
		require.NoError(t, err)
		require.Equal(t, &TraceQLScript{Pipeline: Pipeline{Head: filter(agg("count", ""), ">", lit("2", ""))}}, got)
	})

	t.Run("arithmetic precedence", func(t *testing.T) {
		got, err := Parse(`{} | count() - 1 * 2 ^ 3 ^ 4 > (sum(.a) % 2)`)
		require.NoError(t, err)
		require.Equal(t, `{} | count() - 1 * 2 ^ 3 ^ 4 > (sum(.a) % 2)`, got.String())
		f := got.Pipeline.Stages[0].ScalarFilter
		require.Len(t, f.LHS.Ops, 1)
		pow := f.LHS.Ops[0].Term.Ops[0].Factor
		require.Equal(t, "2", pow.Base.Literal.Num)
		require.Equal(t, "3", pow.Exp.Base.Literal.Num)
		require.Equal(t, "4", pow.Exp.Exp.Base.Literal.Num)
		require.NotNil(t, f.RHS.Head.Head.Base.Paren)
	})

	for _, q := range []string{
		`{} | count() > "2"`,
		`{} | count() > 2 && avg(.a) > 1`,
		`{} | count() > 2 || avg(.a) > 1`,
		`{} | (count() > 2)`,
		`(count() > 2)`,
		`{} | (count() > 2) | avg(.a) > 1`,
		`count() > 3 && { true }`,
		`{} | median(duration) > 2`,
		`{} | count(.a) > 2`,
		`{} | avg() > 2`,
		`{} | count() > 2 | count() >`,
	} {
		t.Run("rejects "+q, func(t *testing.T) {
			_, err := Parse(q)
			require.Error(t, err)
		})
	}
}

func TestParsePipelines(t *testing.T) {
	a := func() Operand { return sel(term(".a", "=", str(`"b"`))) }
	c := func() Operand { return sel(term(".c", "=", str(`"d"`))) }
	count := filter(agg("count", ""), ">", lit("2", ""))
	for _, tc := range []struct {
		query string
		want  *TraceQLScript
	}{
		// A spanset expression at any position.
		{`{.a = "b"} | {.c = "d"}`, script(expr(a()), spanset(expr(c())))},
		{`{.a = "b"} | count() > 2 | {.c = "d"} > {}`, script(expr(a()), count,
			spanset(expr(c(), then(">", sel(nil)))))},
		// A parenthesised pipeline is an operand.
		{`({.a = "b"} | count() > 2) >> {.c = "d"}`, script(expr(paren(expr(a()), count), then(">>", c())))},
		{`{.c = "d"} && ({.a = "b"} | count() > 2)`, script(expr(c(), then("&&", paren(expr(a()), count))))},
		{`({.a = "b"} | count() > 2) | count() > 2`, script(expr(paren(expr(a()), count)), count)},
		{`(({.a = "b"} | count() > 2))`, script(expr(paren(expr(paren(expr(a()), count)))))},
		{`{.a = "b"} | ({.c = "d"} | count() > 2)`, script(expr(a()), spanset(expr(paren(expr(c()), count))))},
	} {
		t.Run(tc.query, func(t *testing.T) {
			got, err := Parse(tc.query)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestParseGroupingStages(t *testing.T) {
	a := func() Operand { return sel(term(".a", "=", str(`"b"`))) }
	count := filter(agg("count", ""), ">", lit("2", ""))
	for _, tc := range []struct {
		query string
		want  *TraceQLScript
	}{
		{`{.a = "b"} | by(.x)`, script(expr(a()), groupBy(field(name(".x"))))},
		{`{} | by(resource.service.name) | by(.x + 1) | count() > 2`, script(expr(sel(nil)),
			groupBy(field(name("resource.service.name"))), groupBy(field(name(".x"), "+", num("1"))), count)},
		{`{} | by(.x) | coalesce() | count() > 2`, script(expr(sel(nil)), groupBy(field(name(".x"))), coalesce(), count)},
		{`{} | by(.x = nil)`, script(expr(sel(nil)), groupBy(term(".x", "=", name("nil"))))},
		{`({.a = "b"} | by(.x))`, script(expr(paren(expr(a()), groupBy(field(name(".x"))))))},
		{`({.a = "b"} | by(.x) | coalesce()) >> {}`, script(expr(paren(expr(a()), groupBy(field(name(".x"))), coalesce()),
			then(">>", sel(nil))))},
		{`by(.x)`, &TraceQLScript{Pipeline: Pipeline{Head: groupBy(field(name(".x")))}}},
		{`by(.x) | {.a = "b"}`, &TraceQLScript{Pipeline: Pipeline{Head: groupBy(field(name(".x"))), Stages: []PipelineElement{spanset(expr(a()))}}}},
		// A leading coalesce() parses; lowering rejects it.
		{`coalesce()`, &TraceQLScript{Pipeline: Pipeline{Head: coalesce()}}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			got, err := Parse(tc.query)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.query, got.String())
		})
	}

	for _, q := range []string{
		`{} | by(.a, .b)`,
		`{} | by .a`,
		`{} | by(.a) && {}`,
		`(by(.a) | {})`,
	} {
		t.Run("rejects "+q, func(t *testing.T) {
			_, err := Parse(q)
			require.Error(t, err)
		})
	}
}

// select() is a pipeline element at any position; its arguments parse as
// field expressions, which lowering restricts to attributes and intrinsics.
func TestParseSelectStages(t *testing.T) {
	a := func() Operand { return sel(term(".a", "=", str(`"b"`))) }
	for _, tc := range []struct {
		query string
		want  *TraceQLScript
	}{
		{`{.a = "b"} | select(.x)`, script(expr(a()), selectOf(field(name(".x"))))},
		{`{} | select(span.x, resource.y, duration, span:name) | count() > 2`, script(expr(sel(nil)),
			selectOf(field(name("span.x")), field(name("resource.y")), field(name("duration")), field(name("span:name"))),
			filter(agg("count", ""), ">", lit("2", "")))},
		{`select(.x)`, &TraceQLScript{Pipeline: Pipeline{Head: selectOf(field(name(".x")))}}},
		{`select(.x) | {.a = "b"}`, &TraceQLScript{Pipeline: Pipeline{Head: selectOf(field(name(".x"))), Stages: []PipelineElement{spanset(expr(a()))}}}},
		{`({.a = "b"} | select(.x)) >> {}`, script(expr(paren(expr(a()), selectOf(field(name(".x")))), then(">>", sel(nil))))},
		{`{} | select(.x + 1)`, script(expr(sel(nil)), selectOf(field(name(".x"), "+", num("1"))))},
	} {
		t.Run(tc.query, func(t *testing.T) {
			got, err := Parse(tc.query)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.query, got.String())
		})
	}

	for _, q := range []string{`{} | select .a`, `{} | select(.a) && {}`, `select()`} {
		t.Run("rejects "+q, func(t *testing.T) {
			_, err := Parse(q)
			require.Error(t, err)
		})
	}
}

func TestParseMetricsFunctions(t *testing.T) {
	a := func() *SpansetExpr { return expr(sel(term(".a", "=", str(`"b"`)))) }
	empty := func() *SpansetExpr { return expr(sel(nil)) }
	metrics := func(e *SpansetExpr, m *MetricsPipelineStage, stages ...PipelineElement) *TraceQLScript {
		res := script(e, stages...)
		res.MetricsFn = m
		return res
	}
	for _, c := range []struct {
		query string
		want  *TraceQLScript
	}{
		{`{.a = "b"} | rate()`, metrics(a(), &MetricsPipelineStage{Fn: "rate"})},
		{`{} | count_over_time() by (span.x, name)`, metrics(empty(),
			&MetricsPipelineStage{Fn: "count_over_time", By: &ByClause{Labels: []string{"span.x", "name"}}})},
		{`{} | quantile_over_time(duration, 0.9)`, metrics(empty(),
			&MetricsPipelineStage{Fn: "quantile_over_time", Attr: "duration", Percentile: f64(0.9)})},
		{`{} | histogram_over_time(duration)`, metrics(empty(),
			&MetricsPipelineStage{Fn: "histogram_over_time", Attr: "duration"})},
		{`{} | compare({.a = "b"}, 10, 1, 2)`, metrics(empty(), &MetricsPipelineStage{Fn: "compare", CompSel: &CompareSelector{
			Filter: term(".a", "=", str(`"b"`)), Count: 10, BaselineFrom: i64(1), BaselineTo: i64(2),
		}})},
		{`{} | compare({})`, metrics(empty(), &MetricsPipelineStage{Fn: "compare", CompSel: &CompareSelector{}})},
		{`{.a = "b"} | count() > 2 | rate()`, metrics(a(), &MetricsPipelineStage{Fn: "rate"},
			filter(agg("count", ""), ">", lit("2", "")))},
		{`count() > 2 | rate()`, &TraceQLScript{
			Pipeline:  Pipeline{Head: filter(agg("count", ""), ">", lit("2", ""))},
			MetricsFn: &MetricsPipelineStage{Fn: "rate"},
		}},
		{`({} &>> {}) | rate()`, metrics(expr(paren(expr(sel(nil), then("&>>", sel(nil))))),
			&MetricsPipelineStage{Fn: "rate"})},
		{`{} | by(.a) | rate() by (name)`, metrics(empty(),
			&MetricsPipelineStage{Fn: "rate", By: &ByClause{Labels: []string{"name"}}}, groupBy(field(name(".a"))))},
		{`{} | by(.a) | coalesce() | count_over_time()`, metrics(empty(),
			&MetricsPipelineStage{Fn: "count_over_time"}, groupBy(field(name(".a"))), coalesce())},
		{`{} | select(.a) | rate()`, metrics(empty(), &MetricsPipelineStage{Fn: "rate"}, selectOf(field(name(".a"))))},
		{`{} && {} | rate()`, metrics(expr(sel(nil), then("&&", sel(nil))), &MetricsPipelineStage{Fn: "rate"})},
	} {
		t.Run(c.query, func(t *testing.T) {
			got, err := Parse(c.query)
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}

	for _, q := range []string{`({} | rate())`, `{} > ({} | rate())`} {
		t.Run("rejects "+q, func(t *testing.T) {
			_, err := Parse(q)
			require.Error(t, err)
		})
	}
}

func TestParseWithHintsAndSecondStage(t *testing.T) {
	rate := func(rest *TraceQLScript) *TraceQLScript {
		rest.Pipeline = Pipeline{Head: spanset(expr(sel(nil)))}
		rest.MetricsFn = &MetricsPipelineStage{Fn: "rate"}
		return rest
	}
	for _, c := range []struct {
		query string
		want  *TraceQLScript
	}{
		{`{} | rate() with(sample=0.1, exemplars=false)`, rate(&TraceQLScript{
			WithHints: &WithClause{Hints: []WithHint{{Key: "sample", Value: "0.1"}, {Key: "exemplars", Value: "false"}}},
		})},
		{`{} | rate() with(sample=true, exemplars=5, most_recent=x)`, rate(&TraceQLScript{
			WithHints: &WithClause{Hints: []WithHint{
				{Key: "sample", Value: "true"}, {Key: "exemplars", Value: "5"}, {Key: "most_recent", Value: "x"},
			}},
		})},
		{`{} | rate() | topk(5)`, rate(&TraceQLScript{
			SecondStage: &SecondPipelineStage{Fn: "topk", Count: 5},
		})},
		{`{} | rate() | bottomk(3) with(sample=0.5)`, rate(&TraceQLScript{
			SecondStage: &SecondPipelineStage{Fn: "bottomk", Count: 3,
				WithHints: &WithClause{Hints: []WithHint{{Key: "sample", Value: "0.5"}}}},
		})},
		{`{} | rate() | select(span.a, duration)`, rate(&TraceQLScript{
			SecondStage: &SecondPipelineStage{Fn: "select", Labels: []string{"span.a", "duration"}},
		})},
		{`{} | rate() with(sample=0.1) | topk(1)`, rate(&TraceQLScript{
			WithHints:   &WithClause{Hints: []WithHint{{Key: "sample", Value: "0.1"}}},
			SecondStage: &SecondPipelineStage{Fn: "topk", Count: 1},
		})},
	} {
		t.Run(c.query, func(t *testing.T) {
			got, err := Parse(c.query)
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}
}

func TestParseRejects(t *testing.T) {
	for _, q := range []string{
		``,
		`{`,
		`{.a = "b"`,
		`{.a = "b"} {.c = "d"}`,
		`({.a = "b"}) ({.c = "d"})`,
	} {
		t.Run(q, func(t *testing.T) {
			_, err := Parse(q)
			require.Error(t, err)
		})
	}
}

func TestParseConcurrent(t *testing.T) {
	queries := []string{`{.a = "b"} &>> {.c = "d"}`, `{} | rate() by (name)`, `{duration > 1s} | count() > 2`}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(q string) {
			defer wg.Done()
			_, err := Parse(q)
			require.NoError(t, err)
		}(queries[i%len(queries)])
	}
	wg.Wait()
}
