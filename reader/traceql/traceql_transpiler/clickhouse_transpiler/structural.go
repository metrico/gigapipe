package clickhouse_transpiler

import (
	"errors"
	"fmt"
	"strings"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	"github.com/metrico/qryn/v5/reader/traceql/traceqlconfig"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

var errStructuralVersion = errors.New(
	"TraceQL structural operators need ClickHouse 24.9 or newer")

// zeroSpanID is the parent of a root span.
const zeroSpanID = "toFixedString('', 8)"

// unresolvedRow is a jump row at the last round whose chain has no known end:
// a parent cycle or a chain deeper than the round limit.
const unresolvedRow = "anc != " + zeroSpanID

// spanResult is which spans a structural operator returns.
type spanResult int

const (
	// resultRHS returns the matched RHS spans.
	resultRHS spanResult = iota
	// resultUnion returns the matched RHS and LHS spans.
	resultUnion
	// resultNot returns the RHS spans without a match.
	resultNot
)

// cond is the SQL condition on a row of the given side (1 for LHS, 2 for RHS)
// that keeps it, given the conditions that an RHS and an LHS row matched.
func (r spanResult) cond(rhsMatched, lhsMatched string) string {
	switch r {
	case resultUnion:
		return fmt.Sprintf("(side = 2 AND %s) OR (side = 1 AND %s)", rhsMatched, lhsMatched)
	case resultNot:
		return fmt.Sprintf("side = 2 AND NOT (%s)", rhsMatched)
	}
	return fmt.Sprintf("side = 2 AND %s", rhsMatched)
}

// hopKey is the id a span is related by.
type hopKey int

const (
	keySpan hopKey = iota
	keyParent
)

// oneHop relates a LHS and a RHS span with equal keys; siblings must differ.
type oneHop struct {
	lhsKey, rhsKey hopKey
	sibling        bool
	result         spanResult
}

var oneHopOps = map[traceql_ir.SpansetOperator]oneHop{
	traceql_ir.OpChild:        {lhsKey: keySpan, rhsKey: keyParent, result: resultRHS},
	traceql_ir.OpParent:       {lhsKey: keyParent, rhsKey: keySpan, result: resultRHS},
	traceql_ir.OpSibling:      {lhsKey: keyParent, rhsKey: keyParent, sibling: true, result: resultRHS},
	traceql_ir.OpNotChild:     {lhsKey: keySpan, rhsKey: keyParent, result: resultNot},
	traceql_ir.OpNotParent:    {lhsKey: keyParent, rhsKey: keySpan, result: resultNot},
	traceql_ir.OpNotSibling:   {lhsKey: keyParent, rhsKey: keyParent, sibling: true, result: resultNot},
	traceql_ir.OpUnionChild:   {lhsKey: keySpan, rhsKey: keyParent, result: resultUnion},
	traceql_ir.OpUnionParent:  {lhsKey: keyParent, rhsKey: keySpan, result: resultUnion},
	traceql_ir.OpUnionSibling: {lhsKey: keyParent, rhsKey: keyParent, sibling: true, result: resultUnion},
}

// StructuralQueryPlanner runs Main, a plan with structural operators, on
// servers that support them, with the analyzer they need.
type StructuralQueryPlanner struct {
	Main shared.SQLRequestPlanner
}

func (s *StructuralQueryPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	if !ctx.VersionInfo.HasCapability(dbversion.CapStructuralQueries) {
		return nil, errStructuralVersion
	}
	main, err := s.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	return main.SetSetting("enable_analyzer", "1"), nil
}

// OneHopPlanner relates the span rows of LHS and RHS by parent or sibling.
// Only spans that reach a root relate.
type OneHopPlanner struct {
	Op     traceql_ir.SpansetOperator
	LHS    shared.SQLRequestPlanner
	RHS    shared.SQLRequestPlanner
	Prefix string
}

func (o *OneHopPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	rel, ok := oneHopOps[o.Op]
	if !ok {
		return nil, fmt.Errorf("%s is not a one-hop operator", o.Op)
	}
	alias := o.Prefix + "hop"
	lhs, rhs, err := processSides(ctx, o.LHS, o.RHS, alias)
	if err != nil {
		return nil, err
	}
	reach := reachableSpans(ctx, alias, lhs, rhs)

	// keyed tags each operand span with its side, reachability and key.
	key := func(k hopKey) string {
		if k == keyParent {
			return fmt.Sprintf("ifNull(r.parent, %s)", zeroSpanID)
		}
		return "o.span_id"
	}
	keyed := sql.NewWith(sql.NewSelect().
		Select(
			sql.NewSimpleCol("o.trace_id", "trace_id"),
			sql.NewSimpleCol("o.span_id", "span_id"),
			sql.NewSimpleCol("o.timestamp_ns", "timestamp_ns"),
			sql.NewSimpleCol("o.side", "side"),
			sql.NewSimpleCol("ifNull(r.reachable, 0)", "reachable"),
			sql.NewSimpleCol(fmt.Sprintf("if(o.side = 1, %s, %s)", key(rel.lhsKey), key(rel.rhsKey)), "key")).
		From(sql.NewCol(&union{selects: []sql.ISelect{
			spanRows(lhs, sql.NewSimpleCol("toUInt8(1)", "side")),
			spanRows(rhs, sql.NewSimpleCol("toUInt8(2)", "side")),
		}}, "o")).
		Join(sql.NewJoin("left", sql.NewCol(sql.NewWithRef(reach), "r"), sameSpan("r.span_id", "o.span_id"))),
		alias+"_keyed")

	// lhs_n and rhs_n count each side's spans per key; self_* discount the span.
	group := "PARTITION BY trace_id, reachable, key"
	cols := append(spanRowCols(),
		sql.NewSimpleCol("side", "side"),
		sql.NewSimpleCol("reachable", "reachable"),
		sql.NewSimpleCol(fmt.Sprintf("countIf(side = 1) OVER (%s)", group), "lhs_n"),
		sql.NewSimpleCol(fmt.Sprintf("countIf(side = 2) OVER (%s)", group), "rhs_n"))
	selfLHS, selfRHS := "0", "0"
	if rel.sibling {
		cols = append(cols,
			sql.NewSimpleCol("max(side = 1) OVER (PARTITION BY trace_id, span_id)", "self_lhs"),
			sql.NewSimpleCol("max(side = 2) OVER (PARTITION BY trace_id, span_id)", "self_rhs"))
		selfLHS, selfRHS = "self_lhs", "self_rhs"
	}
	counted := sql.NewWith(sql.NewSelect().Select(cols...).From(sql.NewWithRef(keyed)), alias+"_counted")

	cond := rel.result.cond(
		fmt.Sprintf("reachable = 1 AND lhs_n > %s", selfLHS),
		fmt.Sprintf("reachable = 1 AND rhs_n > %s", selfRHS))
	return sql.NewSelect().
		With(lhs, rhs).
		AddWith(reach, keyed, counted).
		Distinct(true).
		Select(spanRowCols()...).
		From(sql.NewWithRef(counted)).
		AndWhere(rawCond(cond)), nil
}

// transitive relates a RHS span to a LHS span among its ancestors
// (descendant) or its descendants (ancestor).
type transitive struct {
	descendant bool
	result     spanResult
}

var transitiveOps = map[traceql_ir.SpansetOperator]transitive{
	traceql_ir.OpDescendant:      {descendant: true, result: resultRHS},
	traceql_ir.OpAncestor:        {result: resultRHS},
	traceql_ir.OpNotDescendant:   {descendant: true, result: resultNot},
	traceql_ir.OpNotAncestor:     {result: resultNot},
	traceql_ir.OpUnionDescendant: {descendant: true, result: resultUnion},
	traceql_ir.OpUnionAncestor:   {result: resultUnion},
}

// TransitivePlanner relates the span rows of LHS and RHS by ancestry. Only
// spans that reach a root relate.
type TransitivePlanner struct {
	Op     traceql_ir.SpansetOperator
	LHS    shared.SQLRequestPlanner
	RHS    shared.SQLRequestPlanner
	Prefix string
}

func (t *TransitivePlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	rel, ok := transitiveOps[t.Op]
	if !ok {
		return nil, fmt.Errorf("%s is not a transitive operator", t.Op)
	}
	alias := t.Prefix + "transitive"
	lhs, rhs, err := processSides(ctx, t.LHS, t.RHS, alias)
	if err != nil {
		return nil, err
	}
	sides := sql.NewWith(sql.NewSelect().
		Select(append(spanRowCols(), sql.NewSimpleCol("side", "side"))...).
		From(sql.NewCol(&union{selects: []sql.ISelect{
			spanRows(lhs, sql.NewSimpleCol("toUInt8(1)", "side")),
			spanRows(rhs, sql.NewSimpleCol("toUInt8(2)", "side")),
		}}, alias+"_operands")),
		alias+"_sides")

	// A descendant relation matches RHS spans by up (an LHS ancestor) and LHS
	// spans by down (an RHS descendant); an ancestor relation the other way.
	upSide, downSide, rhsFlag, lhsFlag := 1, 2, "up", "down"
	if !rel.descendant {
		upSide, downSide, rhsFlag, lhsFlag = 2, 1, "down", "up"
	}
	needUp := rel.descendant || rel.result == resultUnion
	needDown := !rel.descendant || rel.result == resultUnion
	resolved := resolvedAncestry(ctx, alias, lhs, rhs, sides, upSide, downSide, needUp, needDown)

	matchedCols := []sql.SQLObject{
		sql.NewSimpleCol("s.trace_id", "trace_id"),
		sql.NewSimpleCol("s.span_id", "span_id"),
		sql.NewSimpleCol("s.timestamp_ns", "timestamp_ns"),
		sql.NewSimpleCol("s.side", "side"),
		sql.NewSimpleCol(fmt.Sprintf("toUInt8(r.reachable = 1 AND r.%s = 1)", rhsFlag), "rhs_match"),
		sql.NewSimpleCol(fmt.Sprintf("toUInt8(r.reachable = 1 AND r.%s = 1)", lhsFlag), "lhs_match"),
	}
	final := rel.result.cond("rhs_match = 1", "lhs_match = 1")
	// Down marks spread through unresolved spans, so an operator reading them
	// returns nothing from a trace with an unresolved span, negations included.
	if needDown {
		matchedCols = append(matchedCols, sql.NewSimpleCol("ifNull(r.unresolved, 0)", "unresolved"))
		final = fmt.Sprintf("unresolved = 0 AND (%s)", final)
	}
	matched := sql.NewWith(sql.NewSelect().
		Select(matchedCols...).
		From(sql.NewCol(sql.NewWithRef(sides), "s")).
		Join(sql.NewJoin("left", sql.NewCol(sql.NewWithRef(resolved), "r"), sameSpan("r.span_id", "s.span_id"))),
		alias+"_matched")

	return sql.NewSelect().
		With(lhs, rhs).
		AddWith(sides, resolved, matched).
		Distinct(true).
		Select(spanRowCols()...).
		From(sql.NewWithRef(matched)).
		AndWhere(rawCond(final)), nil
}

// resolvedAncestry is <alias>_resolved: every span of the candidate traces with
// reachable, up (an upSide ancestor) and down (a downSide descendant); unneeded flags stay 0.
// An unresolved span is not reachable. With needDown, unresolved is set on
// every span of a trace with an unresolved span.
func resolvedAncestry(ctx *shared.PlannerContext, alias string, lhs, rhs, sides *sql.With,
	upSide, downSide int, needUp, needDown bool) *sql.With {
	tree, flags := traceTree(ctx, alias, lhs, rhs)
	rounds := ancestryRounds(ctx)

	members := sql.NewWith(sql.NewSelect().
		Select(
			sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("span_id", "span_id"),
			sql.NewSimpleCol(fmt.Sprintf("max(side = %d)", upSide), "up"),
			sql.NewSimpleCol(fmt.Sprintf("max(side = %d)", downSide), "down")).
		From(sql.NewWithRef(sides)).
		GroupBy(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id")),
		alias+"_members")
	withs := append(tree, flags, members)

	// The anchor holds every span; up marks an up-side parent, down a down-side child.
	cols := []sql.SQLObject{
		sql.NewSimpleCol("f.trace_id", "trace_id"),
		sql.NewSimpleCol("f.span_id", "node"),
		jumpStart("f."),
		sql.NewSimpleCol("f.dangling", "cut"),
		sql.NewSimpleCol("toUInt8(0)", "up"),
		sql.NewSimpleCol("toUInt8(0)", "down"),
		sql.NewSimpleCol("toUInt32(0)", "k"),
	}
	var joins []*sql.Join
	if needUp {
		cols[4] = sql.NewSimpleCol("toUInt8(ifNull(p.up, 0) = 1)", "up")
		joins = append(joins, sql.NewJoin("left", sql.NewCol(sql.NewWithRef(members), "p"), sameSpan("p.span_id", "f.parent")))
	}
	if needDown {
		children := sql.NewWith(sql.NewSelect().
			Select(
				sql.NewSimpleCol("l.trace_id", "trace_id"),
				sql.NewSimpleCol("l.parent", "span_id"),
				sql.NewSimpleCol("toUInt8(1)", "down")).
			From(sql.NewCol(sql.NewWithRef(flags), "l")).
			Join(sql.NewJoin("inner", sql.NewCol(sql.NewWithRef(members), "m"), sameSpan("m.span_id", "l.span_id"))).
			AndWhere(rawCond("m.down = 1")).
			GroupBy(sql.NewRawObject("l.trace_id"), sql.NewRawObject("l.parent")),
			alias+"_children")
		withs = append(withs, children)
		cols[5] = sql.NewSimpleCol("toUInt8(ifNull(c.down, 0) = 1)", "down")
		joins = append(joins, sql.NewJoin("left", sql.NewCol(sql.NewWithRef(children), "c"), sameSpan("c.span_id", "f.span_id")))
	}
	anchor := sql.NewSelect().Select(cols...).From(sql.NewCol(sql.NewWithRef(flags), "f")).Join(joins...)

	// Each round up gains the flags of the skipped ancestors; down marks the
	// ancestors of marked spans (fan-in).
	jump := sql.NewRecursiveWith(anchor, alias+"_jump", func(self *sql.WithRef) sql.ISelect {
		if !needDown {
			return jumpStep(self, rounds, jumpFlag("cut"), jumpFlag("up"), sql.NewSimpleCol("x.down", "down"))
		}
		marked := sql.NewSelect().
			Select(
				sql.NewSimpleCol("trace_id", "trace_id"),
				sql.NewSimpleCol("anc", "node"),
				sql.NewSimpleCol("toUInt8(1)", "hit")).
			From(self).
			AndWhere(rawCond(fmt.Sprintf("down = 1 AND anc != %s", zeroSpanID))).
			GroupBy(sql.NewRawObject("trace_id"), sql.NewRawObject("anc"))
		// The one-select union puts marked in parentheses.
		return jumpStep(self, rounds, jumpFlag("cut"), jumpFlag("up"),
			sql.NewSimpleCol("toUInt8(x.down = 1 OR ifNull(d.hit, 0) = 1)", "down")).
			AddJoin(sql.NewJoin("left",
				sql.NewCol(&union{selects: []sql.ISelect{marked}}, "d"), sameSpan("d.node", "x.node")))
	})

	resolvedCols := []sql.SQLObject{
		sql.NewSimpleCol("trace_id", "trace_id"),
		sql.NewSimpleCol("node", "span_id"),
		sql.NewSimpleCol(fmt.Sprintf("toUInt8(cut = 0 AND NOT (%s))", unresolvedRow), "reachable"),
		sql.NewSimpleCol("up", "up"),
		sql.NewSimpleCol("down", "down"),
	}
	if needDown {
		resolvedCols = append(resolvedCols,
			sql.NewSimpleCol(fmt.Sprintf("max(%s) OVER (PARTITION BY trace_id)", unresolvedRow), "unresolved"))
	}
	return sql.NewWith(sql.NewSelect().
		With(append(withs, jump)...).
		Select(resolvedCols...).
		From(sql.NewWithRef(jump)).
		AndWhere(rawCond(fmt.Sprintf("k = %d", rounds))),
		alias+"_resolved")
}

// reachableSpans is <alias>_reach: the spans that reach a root, with their
// parent, in traces where lhs and rhs both return spans.
func reachableSpans(ctx *shared.PlannerContext, alias string, lhs, rhs *sql.With) *sql.With {
	tree, flags := traceTree(ctx, alias, lhs, rhs)
	rounds := ancestryRounds(ctx)

	// jump runs on traces with a root and a missing parent; cut marks a
	// missing parent on the chain.
	jump := sql.NewRecursiveWith(
		sql.NewSelect().
			Select(
				sql.NewSimpleCol("trace_id", "trace_id"),
				sql.NewSimpleCol("span_id", "node"),
				jumpStart(""),
				sql.NewSimpleCol("dangling", "cut"),
				sql.NewSimpleCol("toUInt32(0)", "k")).
			From(sql.NewWithRef(flags)).
			AndWhere(rawCond("has_root = 1 AND has_dangling = 1")),
		alias+"_jump",
		func(self *sql.WithRef) sql.ISelect { return jumpStep(self, rounds, jumpFlag("cut")) })
	// A cut or unresolved chain reaches no root.
	unreachable := sql.NewWith(sql.NewSelect().
		Select(sql.NewSimpleCol("trace_id", "trace_id"), sql.NewSimpleCol("node", "node")).
		From(sql.NewWithRef(jump)).
		AndWhere(rawCond(fmt.Sprintf("k = %d AND (cut = 1 OR %s)", rounds, unresolvedRow))),
		alias+"_unreachable")

	return sql.NewWith(sql.NewSelect().
		With(append(tree, flags, jump, unreachable)...).
		Select(
			sql.NewSimpleCol("f.trace_id", "trace_id"),
			sql.NewSimpleCol("f.span_id", "span_id"),
			sql.NewSimpleCol("f.parent", "parent"),
			sql.NewSimpleCol("toUInt8(1)", "reachable")).
		From(sql.NewCol(sql.NewWithRef(flags), "f")).
		Join(sql.NewJoin("left anti", sql.NewCol(sql.NewWithRef(unreachable), "u"), sameSpan("u.node", "f.span_id"))).
		AndWhere(rawCond("f.has_root = 1")),
		alias+"_reach")
}

// traceTree returns the CTEs that load the span tree of the traces where lhs
// and rhs both return spans, and <alias>_flags, built on them: every span of
// those traces with its parent, whether it is a root, whether its parent is
// missing (dangling), and whether its trace has a root or a dangling span.
func traceTree(ctx *shared.PlannerContext, alias string, lhs, rhs *sql.With) ([]*sql.With, *sql.With) {
	traceIds := func(w *sql.With) sql.ISelect {
		return sql.NewSelect().Select(sql.NewRawObject("trace_id")).From(sql.NewWithRef(w))
	}
	// edges holds one parent per span, read with no time bound.
	edges := sql.NewWith(sql.NewSelect().
		Select(
			sql.NewSimpleCol("traces.trace_id", "trace_id"),
			sql.NewSimpleCol("traces.span_id", "span_id"),
			sql.NewSimpleCol("any(toFixedString(substring(traces.parent_id, 1, 8), 8))", "parent")).
		From(sql.NewSimpleCol(tracesTable(ctx), "traces")).
		AndWhere(
			inSubquery(ctx, sql.NewRawObject("traces.trace_id"), traceIds(lhs)),
			inSubquery(ctx, sql.NewRawObject("traces.trace_id"), traceIds(rhs))).
		GroupBy(sql.NewRawObject("traces.trace_id"), sql.NewRawObject("traces.span_id")),
		alias+"_edges")

	spans := sql.NewWith(sql.NewSelect().
		Select(
			sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("span_id", "span_id"),
			sql.NewSimpleCol("toUInt8(1)", "found")).
		From(sql.NewWithRef(edges)),
		alias+"_spans")
	links := sql.NewWith(sql.NewSelect().
		Select(
			sql.NewSimpleCol("e.trace_id", "trace_id"),
			sql.NewSimpleCol("e.span_id", "span_id"),
			sql.NewSimpleCol("e.parent", "parent"),
			sql.NewSimpleCol(fmt.Sprintf("toUInt8(e.parent = %s)", zeroSpanID), "is_root"),
			sql.NewSimpleCol(fmt.Sprintf("toUInt8(e.parent != %s AND ifNull(p.found, 0) = 0)", zeroSpanID), "dangling")).
		From(sql.NewCol(sql.NewWithRef(edges), "e")).
		Join(sql.NewJoin("left", sql.NewCol(sql.NewWithRef(spans), "p"), sameSpan("p.span_id", "e.parent"))),
		alias+"_links")
	flags := sql.NewWith(sql.NewSelect().
		Select(
			sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("span_id", "span_id"),
			sql.NewSimpleCol("parent", "parent"),
			sql.NewSimpleCol("is_root", "is_root"),
			sql.NewSimpleCol("dangling", "dangling"),
			sql.NewSimpleCol("max(is_root) OVER (PARTITION BY trace_id)", "has_root"),
			sql.NewSimpleCol("max(dangling) OVER (PARTITION BY trace_id)", "has_dangling")).
		From(sql.NewWithRef(links)),
		alias+"_flags")
	return []*sql.With{edges, spans, links}, flags
}

// jumpStart is a jump anchor's anc: the parent of the flags row with the given
// column prefix, zero for a root or a missing parent.
func jumpStart(prefix string) sql.SQLObject {
	return sql.NewSimpleCol(fmt.Sprintf("if(%[1]sis_root = 1 OR %[1]sdangling = 1, %[2]s, %[1]sparent)", prefix, zeroSpanID), "anc")
}

// jumpStep is one round of a pointer jump over self: row x joined to y, the row
// of x.anc. anc becomes the ancestor twice as far (zero past the chain's end),
// then come the given columns and k.
func jumpStep(self *sql.WithRef, rounds int, cols ...sql.SQLObject) sql.ISelect {
	return sql.NewSelect().
		Select(append(append([]sql.SQLObject{
			sql.NewSimpleCol("x.trace_id", "trace_id"),
			sql.NewSimpleCol("x.node", "node"),
			sql.NewSimpleCol(fmt.Sprintf("if(x.anc = %[1]s, %[1]s, ifNull(y.anc, %[1]s))", zeroSpanID), "anc"),
		}, cols...), sql.NewSimpleCol("toUInt32(x.k + 1)", "k"))...).
		From(sql.NewCol(self, "x")).
		Join(sql.NewJoin("left", sql.NewCol(self, "y"), sameSpan("y.node", "x.anc"))).
		AndWhere(sql.Lt(sql.NewRawObject("x.k"), sql.NewIntVal(int64(rounds))))
}

// jumpFlag is a flag column of jumpStep: set on x or on its ancestor y.
func jumpFlag(col string) sql.SQLObject {
	return sql.NewSimpleCol(fmt.Sprintf("toUInt8(x.%[1]s = 1 OR (x.anc != %[2]s AND ifNull(y.%[1]s, 0) = 1))",
		col, zeroSpanID), col)
}

// ancestryRounds is the number of pointer-jumping rounds: chains up to 2^R
// spans deep resolve.
func ancestryRounds(ctx *shared.PlannerContext) int {
	if ctx.AncestryRounds > 0 {
		return ctx.AncestryRounds
	}
	return traceqlconfig.DefaultAncestryRounds
}

// sameSpan joins two aliased rows on trace id and the given span id columns.
func sameSpan(left, right string) sql.SQLCondition {
	alias := func(col string) string { return strings.SplitN(col, ".", 2)[0] }
	return sql.And(
		sql.Eq(sql.NewRawObject(alias(left)+".trace_id"), sql.NewRawObject(alias(right)+".trace_id")),
		sql.Eq(sql.NewRawObject(left), sql.NewRawObject(right)))
}
