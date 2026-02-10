package traceql_transpiler_v2

import (
	"fmt"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

// AllSpansPlanner matches all spans (empty filter {}).
type AllSpansPlanner struct{}

func (a *AllSpansPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	table := ctx.TracesAttrsTable
	if ctx.IsCluster {
		table = ctx.TracesAttrsDistTable
	}

	return sql.NewSelect().
		Select(
			sql.NewRawObject("trace_id"),
			sql.NewCol(sql.NewRawObject("groupArray(span_id)"), "span_id"),
		).
		From(sql.NewSimpleCol(table, "traces_idx")).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewRawObject(fmt.Sprintf("toDate('%s')", ctx.From.Format("2006-01-02")))),
			sql.Le(sql.NewRawObject("date"), sql.NewRawObject(fmt.Sprintf("toDate('%s')", ctx.To.Format("2006-01-02")))),
			sql.Ge(sql.NewRawObject("traces_idx.timestamp_ns"), sql.NewIntVal(ctx.From.UnixNano())),
			sql.Lt(sql.NewRawObject("traces_idx.timestamp_ns"), sql.NewIntVal(ctx.To.UnixNano())),
		).
		GroupBy(sql.NewRawObject("trace_id")), nil
}

// SingleConditionPlanner handles a single condition.
type SingleConditionPlanner struct {
	Condition SQLConditionPlanner
}

func (s *SingleConditionPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	table := ctx.TracesAttrsTable
	if ctx.IsCluster {
		table = ctx.TracesAttrsDistTable
	}

	cond, err := s.Condition.ToCondition(ctx)
	if err != nil {
		return nil, err
	}

	query := sql.NewSelect().
		Select(
			sql.NewRawObject("trace_id"),
			sql.NewCol(sql.NewRawObject("groupArray(span_id)"), "span_id"),
		).
		From(sql.NewSimpleCol(table, "traces_idx")).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewRawObject(fmt.Sprintf("toDate('%s')", ctx.From.Format("2006-01-02")))),
			sql.Le(sql.NewRawObject("date"), sql.NewRawObject(fmt.Sprintf("toDate('%s')", ctx.To.Format("2006-01-02")))),
			sql.Ge(sql.NewRawObject("traces_idx.timestamp_ns"), sql.NewIntVal(ctx.From.UnixNano())),
			sql.Lt(sql.NewRawObject("traces_idx.timestamp_ns"), sql.NewIntVal(ctx.To.UnixNano())),
			cond, // All conditions go to WHERE - they filter rows before grouping
		).
		GroupBy(sql.NewRawObject("trace_id"))

	return query, nil
}

// MultiConditionPlanner handles multiple conditions with AND/OR.
type MultiConditionPlanner struct {
	Conditions []SQLConditionPlanner
	Op         string // "AND" or "OR"
}

func (m *MultiConditionPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	table := ctx.TracesAttrsTable
	if ctx.IsCluster {
		table = ctx.TracesAttrsDistTable
	}

	// Build combined condition
	var sqlConds []sql.SQLCondition
	for _, cond := range m.Conditions {
		sqlCond, err := cond.ToCondition(ctx)
		if err != nil {
			return nil, err
		}
		sqlConds = append(sqlConds, sqlCond)
	}

	var combined sql.SQLCondition
	if m.Op == "AND" {
		combined = sql.And(sqlConds...)
	} else {
		combined = sql.Or(sqlConds...)
	}

	query := sql.NewSelect().
		Select(
			sql.NewRawObject("trace_id"),
			sql.NewCol(sql.NewRawObject("groupArray(span_id)"), "span_id"),
		).
		From(sql.NewSimpleCol(table, "traces_idx")).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewRawObject(fmt.Sprintf("toDate('%s')", ctx.From.Format("2006-01-02")))),
			sql.Le(sql.NewRawObject("date"), sql.NewRawObject(fmt.Sprintf("toDate('%s')", ctx.To.Format("2006-01-02")))),
			sql.Ge(sql.NewRawObject("traces_idx.timestamp_ns"), sql.NewIntVal(ctx.From.UnixNano())),
			sql.Lt(sql.NewRawObject("traces_idx.timestamp_ns"), sql.NewIntVal(ctx.To.UnixNano())),
			combined, // All conditions go to WHERE - they filter rows before grouping
		).
		GroupBy(sql.NewRawObject("trace_id"))

	return query, nil
}

// IndexLimitPlanner wraps a planner with LIMIT.
type IndexLimitPlanner struct {
	Main shared.SQLRequestPlanner
}

func (i *IndexLimitPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := i.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	if ctx.Limit > 0 {
		main = main.Limit(sql.NewIntVal(ctx.Limit))
	}

	return main, nil
}
