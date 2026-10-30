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

// col is the key column of a resolved one-hop row; a span that does not
// reach a root has a zero parent key.
func (k hopKey) col() string {
	if k == keyParent {
		return fmt.Sprintf("if(reachable = 1, parent, %s)", zeroSpanID)
	}
	return "span_id"
}

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
	ctes, keyed := oneHopKeyed(ctx, alias, lhs, rhs, rel)

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
		AddWith(append(ctes, keyed, counted)...).
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

// direction is the ancestry flags rel reads and the flag columns that match
// its RHS and LHS spans: a descendant relation matches RHS by up, LHS by down.
func (rel transitive) direction() (f ancestryFlags, rhsFlag, lhsFlag string) {
	union := rel.result == resultUnion
	f = ancestryFlags{up: rel.descendant || union, down: !rel.descendant || union}
	if rel.descendant {
		f.upSide, f.downSide = 1, 2
		return f, "up", "down"
	}
	f.upSide, f.downSide = 2, 1
	return f, "down", "up"
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
	flags, rhsFlag, lhsFlag := rel.direction()
	sides := operandSides(alias, lhs, rhs)
	nodes, jump := ancestryTree(ctx, alias, sides, flags, scopeSpans)
	resolved := sql.NewWith(sql.NewSelect().
		Select(
			sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("node", "span_id"),
			sql.NewSimpleCol("cut", "cut"),
			sql.NewSimpleCol("up", "up"),
			sql.NewSimpleCol("down", "down"),
			sql.NewSimpleCol("lhs_ts", "lhs_ts"),
			sql.NewSimpleCol("rhs_ts", "rhs_ts")).
		From(sql.NewWithRef(jump)).
		AndWhere(ancestryGuard(ctx, finalJump), rawCond(finalJump)),
		alias+"_resolved")
	matched := sql.NewWith(sql.NewSelect().
		Select(
			sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("span_id", "span_id"),
			sql.NewSimpleCol("o.1", "timestamp_ns"),
			sql.NewSimpleCol("o.2", "side"),
			sql.NewSimpleCol(fmt.Sprintf("toUInt8(cut = 0 AND %s = 1)", rhsFlag), "rhs_match"),
			sql.NewSimpleCol(fmt.Sprintf("toUInt8(cut = 0 AND %s = 1)", lhsFlag), "lhs_match")).
		From(sql.NewWithRef(resolved)).
		Join(sql.NewJoin("array", sql.NewRawObject(operandRows), nil)),
		alias+"_matched")
	return sql.NewSelect().
		With(lhs, rhs).
		AddWith(sides, nodes, jump, resolved, matched).
		Distinct(true).
		Select(spanRowCols()...).
		From(sql.NewWithRef(matched)).
		AndWhere(rawCond(rel.result.cond("rhs_match = 1", "lhs_match = 1"))), nil
}

// operandSides is <alias>_sides: the span rows of lhs (side 1) and rhs (side 2).
func operandSides(alias string, lhs, rhs *sql.With) *sql.With {
	return sql.NewWith(sql.NewSelect().
		Select(append(spanRowCols(), sql.NewSimpleCol("side", "side"))...).
		From(sql.NewCol(&union{selects: []sql.ISelect{
			spanRows(lhs, sql.NewSimpleCol("toUInt8(1)", "side")),
			spanRows(rhs, sql.NewSimpleCol("toUInt8(2)", "side")),
		}}, alias+"_operands")),
		alias+"_sides")
}

// ancestryFlags are what a pointer jump flags on a span: up, a proper
// ancestor of side upSide, and down, a proper descendant of side downSide.
type ancestryFlags struct {
	upSide, downSide int
	up, down         bool
}

// treeScope is which spans a pointer jump covers and which traces recurse.
type treeScope int

const (
	// scopeSpans covers every span and recurses over every trace.
	scopeSpans treeScope = iota
	// scopeIncomplete also covers missing parents and recurses over traces
	// with a root and a missing parent.
	scopeIncomplete
)

// parentID is the parent span id of a traces row.
const parentID = "toFixedString(substring(traces.parent_id, 1, 8), 8)"

// hasRoot is whether a node's trace has a found root.
var hasRoot = fmt.Sprintf("max(found = 1 AND parent = %s) OVER (PARTITION BY trace_id)", zeroSpanID)

// edges reads the cand traces' spans with their parents, as side 0; under
// scopeIncomplete each parent is also a side 3 row.
func (s treeScope) edges(ctx *shared.PlannerContext, cand sql.ISelect) sql.ISelect {
	spanID, parent, side := "traces.span_id", parentID, "toUInt8(0)"
	sel := sql.NewSelect().
		From(sql.NewSimpleCol(tracesTable(ctx), "traces")).
		AndWhere(inSubquery(ctx, sql.NewRawObject("traces.trace_id"), cand))
	if s == scopeIncomplete {
		spanID, parent, side = "e.1", "e.2", "e.3"
		sel = sel.Join(sql.NewJoin("array", sql.NewRawObject(fmt.Sprintf(
			"arrayFilter(e -> e.1 != %[1]s, [(traces.span_id, %[2]s, toUInt8(0)), (%[2]s, %[1]s, toUInt8(3))]) AS e",
			zeroSpanID, parentID)), nil))
	}
	return sel.Select(
		sql.NewSimpleCol("traces.trace_id", "trace_id"),
		sql.NewSimpleCol(spanID, "span_id"),
		sql.NewSimpleCol(parent, "parent"),
		sql.NewSimpleCol("toInt64(0)", "timestamp_ns"),
		sql.NewSimpleCol(side, "side"))
}

// recurse is the anchor condition for a node to take jumps.
func (s treeScope) recurse() string {
	if s == scopeIncomplete {
		return "found = 1 AND " + hasRoot + " = 1 AND max(found = 0) OVER (PARTITION BY trace_id) = 1"
	}
	return "found = 1"
}

// ancestryTree returns <alias>_nodes, the candidate traces' spans plus every
// operand span, and <alias>_jump, a pointer jump flagging each node by f and cut.
func ancestryTree(ctx *shared.PlannerContext, alias string, sides *sql.With,
	f ancestryFlags, scope treeScope) (nodes, jump *sql.With) {
	cand := sql.NewSelect().
		Select(sql.NewRawObject("trace_id")).
		From(sql.NewWithRef(sides)).
		GroupBy(sql.NewRawObject("trace_id")).
		AndHaving(rawCond("max(side = 1) = 1 AND max(side = 2) = 1"))
	operands := sql.NewSelect().
		Select(
			sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("span_id", "span_id"),
			sql.NewSimpleCol(zeroSpanID, "parent"),
			sql.NewSimpleCol("toInt64(timestamp_ns)", "timestamp_ns"),
			sql.NewSimpleCol("side", "side")).
		From(sql.NewWithRef(sides))
	nodes = sql.NewWith(sql.NewSelect().
		Select(
			sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("span_id", "span_id"),
			sql.NewSimpleCol("max(side = 0)", "found"),
			sql.NewSimpleCol("anyIf(parent, side = 0)", "parent"),
			sql.NewSimpleCol(fmt.Sprintf("max(side = %d)", f.upSide), "up_operand"),
			sql.NewSimpleCol(fmt.Sprintf("max(side = %d)", f.downSide), "down_operand"),
			sql.NewSimpleCol("groupUniqArrayIf(timestamp_ns, side = 1)", "lhs_ts"),
			sql.NewSimpleCol("groupUniqArrayIf(timestamp_ns, side = 2)", "rhs_ts")).
		From(sql.NewCol(&union{selects: []sql.ISelect{scope.edges(ctx, cand), operands}}, alias+"_rows")).
		GroupBy(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id")),
		alias+"_nodes")

	// A trace takes one jump per doubling of its found spans; past the limit,
	// one extra jump sets past_limit when the chain still goes on.
	limit := ancestryRounds(ctx)
	needed := "greatest(1, ceil(log2(countIf(found = 1) OVER (PARTITION BY trace_id) + 1)))"
	rounds := fmt.Sprintf("if(%[3]s, toUInt32(if(%[2]s <= %[1]d, %[2]s, %[1]d + 1)), toUInt32(0))",
		limit, needed, scope.recurse())
	anchor := sql.NewSelect().
		Select(
			sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("span_id", "node"),
			sql.NewSimpleCol("parent", "parent"),
			sql.NewSimpleCol(fmt.Sprintf("if(found = 1, parent, %s)", zeroSpanID), "anc"),
			sql.NewSimpleCol("found", "found"),
			sql.NewSimpleCol("toUInt8(found = 0)", "cut"),
			sql.NewSimpleCol("up_operand", "up_operand"),
			sql.NewSimpleCol("down_operand", "down_operand"),
			sql.NewSimpleCol("toUInt8(0)", "up"),
			sql.NewSimpleCol("toUInt8(0)", "down"),
			sql.NewSimpleCol("lhs_ts", "lhs_ts"),
			sql.NewSimpleCol("rhs_ts", "rhs_ts"),
			sql.NewSimpleCol(rounds, "rounds"),
			sql.NewSimpleCol("toUInt8(0)", "past_limit"),
			sql.NewSimpleCol("toUInt32(0)", "jumps")).
		From(sql.NewWithRef(nodes))

	// After j jumps x's flags cover its ancestors 1..2^j-1 away and y's (x.anc's)
	// the next 2^j; an unmatched y is a missing parent.
	jump = sql.NewRecursiveWith(anchor, alias+"_jump", func(self *sql.WithRef) sql.ISelect {
		yFound := "ifNull(y.found, 0) = 1"
		up := sql.NewSimpleCol("x.up", "up")
		if f.up {
			up = sql.NewSimpleCol(fmt.Sprintf(
				"toUInt8(x.up = 1 OR (x.anc != %s AND (ifNull(y.up_operand, 0) = 1 OR ifNull(y.up, 0) = 1)))",
				zeroSpanID), "up")
		}
		down := sql.NewSimpleCol("x.down", "down")
		if f.down {
			down = sql.NewSimpleCol("toUInt8(x.down = 1 OR ifNull(d.hit, 0) = 1)", "down")
		}
		step := sql.NewSelect().
			Select(
				sql.NewSimpleCol("x.trace_id", "trace_id"),
				sql.NewSimpleCol("x.node", "node"),
				sql.NewSimpleCol("x.parent", "parent"),
				sql.NewSimpleCol(fmt.Sprintf("if(x.anc = %[1]s OR NOT (%[2]s), %[1]s, y.anc)", zeroSpanID, yFound), "anc"),
				sql.NewSimpleCol("x.found", "found"),
				sql.NewSimpleCol(fmt.Sprintf(
					"toUInt8(x.cut = 1 OR (x.anc != %s AND (NOT (%s) OR ifNull(y.cut, 0) = 1)))", zeroSpanID, yFound), "cut"),
				sql.NewSimpleCol("x.up_operand", "up_operand"),
				sql.NewSimpleCol("x.down_operand", "down_operand"),
				up,
				down,
				sql.NewSimpleCol("x.lhs_ts", "lhs_ts"),
				sql.NewSimpleCol("x.rhs_ts", "rhs_ts"),
				sql.NewSimpleCol("x.rounds", "rounds"),
				sql.NewSimpleCol(fmt.Sprintf("toUInt8(x.past_limit = 1 OR (x.jumps = %d AND x.anc != %s AND %s))",
					limit, zeroSpanID, yFound), "past_limit"),
				sql.NewSimpleCol("toUInt32(x.jumps + 1)", "jumps")).
			From(sql.NewCol(self, "x")).
			Join(sql.NewJoin("left", sql.NewCol(self, "y"), sameSpan("y.node", "x.anc"))).
			AndWhere(rawCond("x.jumps < x.rounds"))
		if f.down {
			// A downSide span, or one with a downSide descendant, marks its
			// ancestor 2^j away.
			marked := sql.NewSelect().
				Select(
					sql.NewSimpleCol("trace_id", "trace_id"),
					sql.NewSimpleCol("anc", "node"),
					sql.NewSimpleCol("toUInt8(1)", "hit")).
				From(self).
				AndWhere(rawCond(fmt.Sprintf("(down_operand = 1 OR down = 1) AND anc != %s", zeroSpanID))).
				GroupBy(sql.NewRawObject("trace_id"), sql.NewRawObject("anc"))
			step.AddJoin(sql.NewJoin("left",
				sql.NewCol(&union{selects: []sql.ISelect{marked}}, "d"), sameSpan("d.node", "x.node")))
		}
		return step
	})
	return nodes, jump
}

// finalJump keeps a node's row after its last jump.
const finalJump = "jumps = rounds"

// ancestryGuard fails the query on a final jump row whose chain is unresolved
// (an ancestor pointer still set, or past the limit), when cond (SQL) holds.
func ancestryGuard(ctx *shared.PlannerContext, cond string) sql.SQLCondition {
	limit := ancestryRounds(ctx)
	return rawCond(fmt.Sprintf("throwIf(%s AND if(rounds <= %d, anc != %s, past_limit = 1), '%s') = 0",
		cond, limit, zeroSpanID, unresolvedAncestryMessage(limit)))
}

// operandRows ARRAY JOINs a final jump row into its operand rows: o.1 the
// timestamp, o.2 the side.
const operandRows = "arrayConcat(arrayMap(t -> (t, toUInt8(1)), lhs_ts), arrayMap(t -> (t, toUInt8(2)), rhs_ts)) AS o"

// oneHopKeyed returns keyed, every operand row with its side, reachability
// and rel's key, and the CTEs it reads.
func oneHopKeyed(ctx *shared.PlannerContext, alias string, lhs, rhs *sql.With, rel oneHop) (ctes []*sql.With, keyed *sql.With) {
	sides := operandSides(alias, lhs, rhs)
	nodes, jump := ancestryTree(ctx, alias, sides, ancestryFlags{upSide: 1, downSide: 2}, scopeIncomplete)
	final := sql.NewWith(sql.NewSelect().
		Select(
			sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("node", "span_id"),
			sql.NewSimpleCol("parent", "parent"),
			sql.NewSimpleCol("anc", "anc"),
			sql.NewSimpleCol("rounds", "rounds"),
			sql.NewSimpleCol("past_limit", "past_limit"),
			sql.NewSimpleCol("found", "found"),
			sql.NewSimpleCol("cut", "cut"),
			sql.NewSimpleCol("lhs_ts", "lhs_ts"),
			sql.NewSimpleCol("rhs_ts", "rhs_ts"),
			sql.NewSimpleCol(hasRoot, "has_root"),
			sql.NewSimpleCol("max(found = 1 AND cut = 1) OVER (PARTITION BY trace_id)", "has_dangling")).
		From(sql.NewWithRef(jump)).
		AndWhere(rawCond(finalJump)),
		alias+"_final")
	// A span reaches a root when its trace has one and its chain is not cut.
	resolved := sql.NewWith(sql.NewSelect().
		Select(
			sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("span_id", "span_id"),
			sql.NewSimpleCol("parent", "parent"),
			sql.NewSimpleCol("toUInt8(found = 1 AND has_root = 1 AND cut = 0)", "reachable"),
			sql.NewSimpleCol("lhs_ts", "lhs_ts"),
			sql.NewSimpleCol("rhs_ts", "rhs_ts")).
		From(sql.NewWithRef(final)).
		AndWhere(ancestryGuard(ctx, "has_root = 1 AND has_dangling = 1")),
		alias+"_resolved")
	keyed = sql.NewWith(sql.NewSelect().
		Select(
			sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("span_id", "span_id"),
			sql.NewSimpleCol("o.1", "timestamp_ns"),
			sql.NewSimpleCol("o.2", "side"),
			sql.NewSimpleCol("reachable", "reachable"),
			sql.NewSimpleCol(fmt.Sprintf("if(o.2 = 1, %s, %s)", rel.lhsKey.col(), rel.rhsKey.col()), "key")).
		From(sql.NewWithRef(resolved)).
		Join(sql.NewJoin("array", sql.NewRawObject(operandRows), nil)),
		alias+"_keyed")
	return []*sql.With{sides, nodes, jump, final, resolved}, keyed
}

// ancestryRounds is the number of pointer-jumping rounds: chains up to 2^R
// spans deep resolve.
func ancestryRounds(ctx *shared.PlannerContext) int {
	if ctx.AncestryRounds > 0 {
		return ctx.AncestryRounds
	}
	return traceqlconfig.DefaultAncestryRounds
}

func unresolvedAncestryMessage(rounds int) string {
	return fmt.Sprintf("TraceQL structural query: a trace is deeper than the round limit of %d rounds "+
		"(%d spans) or its parent links form a cycle; raise %s",
		rounds, 1<<rounds, traceqlconfig.AncestryRoundsEnv)
}

// unresolvedAncestryError returns the unresolved-ancestry error that err, a
// ClickHouse error from a query planned with ctx, carries, or err itself.
func unresolvedAncestryError(ctx *shared.PlannerContext, err error) error {
	msg := unresolvedAncestryMessage(ancestryRounds(ctx))
	if err != nil && strings.Contains(err.Error(), msg) {
		return errors.New(msg)
	}
	return err
}

// sameSpan joins two aliased rows on trace id and the given span id columns.
func sameSpan(left, right string) sql.SQLCondition {
	alias := func(col string) string { return strings.SplitN(col, ".", 2)[0] }
	return sql.And(
		sql.Eq(sql.NewRawObject(alias(left)+".trace_id"), sql.NewRawObject(alias(right)+".trace_id")),
		sql.Eq(sql.NewRawObject(left), sql.NewRawObject(right)))
}
