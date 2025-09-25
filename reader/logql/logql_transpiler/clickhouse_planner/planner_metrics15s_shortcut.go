package clickhouse_planner

import (
	"fmt"
	"time"

	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/reader/plugins"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type Metrics15ShortcutPlanner struct {
	Function string
	Duration time.Duration
	Offset   *time.Duration
}

func NewMetrics15ShortcutPlanner(function string, duration time.Duration,
	offset *time.Duration) shared.SQLRequestPlanner {
	p := plugins.GetMetrics15ShortcutPlannerPlugin()
	if p != nil {
		return (*p)(function, duration)
	}
	return &Metrics15ShortcutPlanner{
		Function: function,
		Duration: duration,
		Offset:   offset,
	}
}

func (m *Metrics15ShortcutPlanner) GetQuery(ctx *shared.PlannerContext, col sql.SQLObject, table string) sql.ISelect {
	from := ctx.From
	to := ctx.To
	offsetNsStr := ""
	if m.Offset != nil {
		from = from.Add(*m.Offset)
		to = to.Add(*m.Offset)
		offsetNsStr = fmt.Sprintf(" + %d", m.Offset.Nanoseconds())
	}
	return sql.NewSelect().
		Select(
			sql.NewSimpleCol(
				fmt.Sprintf("intDiv(samples.timestamp_ns%s, %d) * %[2]d",
					offsetNsStr,
					m.Duration.Nanoseconds()),
				"timestamp_ns",
			),
			sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol(`''`, "string"),
			sql.NewCol(col, "value")).
		From(sql.NewSimpleCol(table, "samples")).
		AndWhere(
			sql.Ge(sql.NewRawObject("samples.timestamp_ns"),
				sql.NewIntVal(from.UnixNano()/15000000000*15000000000)),
			sql.Lt(sql.NewRawObject("samples.timestamp_ns"),
				sql.NewIntVal((to.UnixNano()/15000000000)*15000000000)),
			GetTypes(ctx)).
		GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("timestamp_ns"))
}

func (m *Metrics15ShortcutPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	var col sql.SQLObject
	switch m.Function {
	case "rate":
		col = sql.NewRawObject(
			fmt.Sprintf("toFloat64(countMerge(count)) / %f",
				float64(m.Duration.Milliseconds())/1000))
	case "count_over_time":
		col = sql.NewRawObject("countMerge(count)")
	}
	v1 := m.GetQuery(ctx, col, ctx.Metrics15sDistTableName)
	return v1, nil
}

/*
type UnionSelect struct {
	sql.ISelect
	SubSelects []sql.ISelect
}

func (u *UnionSelect) Distinct(distinct bool) sql.ISelect {
	u.MainSelect.Distinct(distinct)
	return u
}

func (u *UnionSelect) GetDistinct() bool {
	return u.MainSelect.GetDistinct()
}

func (u *UnionSelect) Select(cols ...sql.SQLObject) sql.ISelect {
	u.MainSelect.Select(cols...)
	return u
}

func (u *UnionSelect) GetSelect() []sql.SQLObject {
	return u.MainSelect.GetSelect()
}

func (u *UnionSelect) From(table sql.SQLObject) sql.ISelect {
	u.MainSelect.From(table)
	return u
}

func (u *UnionSelect) GetFrom() sql.SQLObject {
	return u.MainSelect.GetFrom()
}

func (u *UnionSelect) AndWhere(clauses ...sql.SQLCondition) sql.ISelect {
	for _, s := range u.SubSelects {
		s.AndWhere(clauses...)
	}
	return u
}

func (u *UnionSelect) OrWhere(clauses ...sql.SQLCondition) sql.ISelect {
	u.MainSelect.OrWhere(clauses...)
	return u
}

func (u *UnionSelect) GetWhere() sql.SQLCondition {
	return u.MainSelect.GetWhere()
}

func (u *UnionSelect) AndPreWhere(clauses ...sql.SQLCondition) sql.ISelect {
	u.MainSelect.AndPreWhere(clauses...)
	return u
}

func (u *UnionSelect) OrPreWhere(clauses ...sql.SQLCondition) sql.ISelect {
	u.MainSelect.OrPreWhere(clauses...)
	return u
}

func (u *UnionSelect) GetPreWhere() sql.SQLCondition {
	return u.MainSelect.GetPreWhere()
}

func (u *UnionSelect) AndHaving(clauses ...sql.SQLCondition) sql.ISelect {
	u.MainSelect.AndHaving(clauses...)
	return u
}

func (u *UnionSelect) OrHaving(clauses ...sql.SQLCondition) sql.ISelect {
	u.MainSelect.OrHaving(clauses...)
	return u
}

func (u *UnionSelect) GetHaving() sql.SQLCondition {
	return u.MainSelect.GetHaving()
}

func (u *UnionSelect) SetHaving(having sql.SQLCondition) sql.ISelect {
	u.MainSelect.SetHaving(having)
	return u
}

func (u *UnionSelect) GroupBy(fields ...sql.SQLObject) sql.ISelect {
	u.MainSelect.GroupBy(fields...)
	return u
}

func (u *UnionSelect) GetGroupBy() []sql.SQLObject {
	return u.MainSelect.GetGroupBy()
}

func (u *UnionSelect) OrderBy(fields ...sql.SQLObject) sql.ISelect {
	u.MainSelect.OrderBy(fields...)
	return u
}

func (u *UnionSelect) GetOrderBy() []sql.SQLObject {
	return u.MainSelect.GetOrderBy()
}

func (u *UnionSelect) Limit(limit sql.SQLObject) sql.ISelect {
	u.MainSelect.Limit(limit)
	return u
}

func (u *UnionSelect) GetLimit() sql.SQLObject {
	return u.MainSelect.GetLimit()
}

func (u *UnionSelect) Offset(offset sql.SQLObject) sql.ISelect {
	u.MainSelect.Offset(offset)
	return u
}

func (u *UnionSelect) GetOffset() sql.SQLObject {
	return u.MainSelect.GetOffset()
}

func (u *UnionSelect) With(withs ...*sql.With) sql.ISelect {
	u.MainSelect.With(withs...)
	return u
}

func (u *UnionSelect) AddWith(withs ...*sql.With) sql.ISelect {
	u.MainSelect.AddWith(withs...)
	return u
}

func (u *UnionSelect) DropWith(alias ...string) sql.ISelect {
	u.MainSelect.DropWith(alias...)
	return u
}

func (u *UnionSelect) GetWith() []*sql.With {
	var w []*sql.With = u.MainSelect.GetWith()
	for _, ww := range u.SubSelects {
		w = append(w, ww.GetWith()...)
	}
	return w
}

func (u *UnionSelect) Join(joins ...*sql.Join) sql.ISelect {
	u.MainSelect.Join(joins...)
	return u
}

func (u *UnionSelect) AddJoin(joins ...*sql.Join) sql.ISelect {
	u.MainSelect.AddJoin(joins...)
	return u
}

func (u *UnionSelect) GetJoin() []*sql.Join {
	return u.MainSelect.GetJoin()
}

func (u *UnionSelect) String(ctx *sql.Ctx, options ...int) (string, error) {
	return u.MainSelect.String(ctx, options...)
}

func (u *UnionSelect) SetSetting(name string, value string) sql.ISelect {
	u.MainSelect.SetSetting(name, value)
	return u
}

func (u *UnionSelect) GetSettings(table sql.SQLObject) map[string]string {
	return u.MainSelect.GetSettings(table)
}
*/
