package clickhouse_transpiler

import (
	"fmt"
	"strings"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// TraceAndPlanner is `LHS && RHS`: in traces where both sides return spans,
// the spans of both sides, deduplicated.
type TraceAndPlanner struct {
	LHS    shared.SQLRequestPlanner
	RHS    shared.SQLRequestPlanner
	Prefix string
}

func (t *TraceAndPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	lhs, rhs, err := processSides(ctx, t.LHS, t.RHS, t.Prefix+"and")
	if err != nil {
		return nil, err
	}
	// sides is a bitmask of the operands that return spans in the trace.
	tagged := sql.NewSelect().
		Select(append(spanRowCols(),
			sql.NewSimpleCol("groupBitOr(side) OVER (PARTITION BY trace_id)", "sides"))...).
		From(sql.NewCol(&union{selects: []sql.ISelect{
			spanRows(lhs, sql.NewSimpleCol("1", "side")),
			spanRows(rhs, sql.NewSimpleCol("2", "side")),
		}}, t.Prefix+"and_operands"))
	withTagged := sql.NewWith(tagged, t.Prefix+"and_sides")
	return sql.NewSelect().
		With(lhs, rhs, withTagged).
		Distinct(true).
		Select(spanRowCols()...).
		From(sql.NewWithRef(withTagged)).
		AndWhere(sql.Eq(sql.NewRawObject("sides"), sql.NewIntVal(3))), nil
}

// TraceOrPlanner is `LHS || RHS`: the spans either side returns, deduplicated.
type TraceOrPlanner struct {
	LHS    shared.SQLRequestPlanner
	RHS    shared.SQLRequestPlanner
	Prefix string
}

func (t *TraceOrPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	lhs, rhs, err := processSides(ctx, t.LHS, t.RHS, t.Prefix+"or")
	if err != nil {
		return nil, err
	}
	return sql.NewSelect().
		With(lhs, rhs).
		Distinct(true).
		Select(spanRowCols()...).
		From(sql.NewCol(&union{selects: []sql.ISelect{spanRows(lhs), spanRows(rhs)}}, t.Prefix+"or_operands")), nil
}

// processSides plans both operands as the CTEs <alias>_lhs and <alias>_rhs.
func processSides(ctx *shared.PlannerContext, lhs, rhs shared.SQLRequestPlanner, alias string) (*sql.With, *sql.With, error) {
	l, err := lhs.Process(ctx)
	if err != nil {
		return nil, nil, err
	}
	r, err := rhs.Process(ctx)
	if err != nil {
		return nil, nil, err
	}
	return sql.NewWith(l, alias+"_lhs"), sql.NewWith(r, alias+"_rhs"), nil
}

// spanRowCols are the span-row columns every spanset planner returns.
func spanRowCols() []sql.SQLObject {
	return spanRowColsOf("")
}

// spanRowColsOf are the span-row columns read from the given table alias.
func spanRowColsOf(alias string) []sql.SQLObject {
	prefix := ""
	if alias != "" {
		prefix = alias + "."
	}
	return []sql.SQLObject{
		sql.NewSimpleCol(prefix+"trace_id", "trace_id"),
		sql.NewSimpleCol(prefix+"span_id", "span_id"),
		sql.NewSimpleCol(prefix+"timestamp_ns", "timestamp_ns"),
	}
}

func spanRows(from *sql.With, extra ...sql.SQLObject) sql.ISelect {
	return sql.NewSelect().Select(append(spanRowCols(), extra...)...).From(sql.NewWithRef(from))
}

type union struct {
	selects []sql.ISelect
}

func (u *union) String(ctx *sql.Ctx, opts ...int) (string, error) {
	var _opts []int
	for _, opt := range opts {
		if opt != sql.STRING_OPT_SKIP_WITH {
			_opts = append(_opts, opt)
		}
	}
	strSelects := make([]string, len(u.selects))
	var err error
	for i, s := range u.selects {
		strSelects[i], err = s.String(ctx, _opts...)
		if err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("(%s)", strings.Join(strSelects, " UNION ALL ")), nil
}
