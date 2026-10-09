package clickhouse_planner

import (
	"fmt"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/plugins"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// cellNs is the width of a metrics_15s cell; cell c holds the lines in
// [c, c+15s).
const cellNs = int64(15 * time.Second)

// ShortcutRoute serves a query from metrics_15s when metrics15sServes the
// request, and from its raw SQL plan otherwise.
type ShortcutRoute struct {
	Shortcut shared.SQLRequestPlanner
	Raw      shared.SQLRequestPlanner
	Duration time.Duration
	Offset   time.Duration
}

func (s *ShortcutRoute) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	if metrics15sServes(ctx, s.Duration, s.Offset) {
		return s.Shortcut.Process(ctx)
	}
	return s.Raw.Process(ctx)
}

// metrics15sServes reports whether metrics_15s exists and every evaluation
// time, r and the offset are multiples of 15s.
func metrics15sServes(ctx *shared.PlannerContext, r, offset time.Duration) bool {
	if ctx.Grid == nil {
		return false
	}
	if ctx.VersionInfo != nil && !ctx.VersionInfo.HasCapability(dbversion.CapMetrics15s) {
		return false
	}
	g := ctx.Grid
	return g.FirstNs%cellNs == 0 && g.StepNs%cellNs == 0 &&
		r.Nanoseconds()%cellNs == 0 && offset.Nanoseconds()%cellNs == 0
}

// Metrics15ShortcutPlanner evaluates rate or count_over_time over (T-R, T]
// from the metrics_15s cells of [T-R, T), each raw line exactly on T or T-R
// moved one cell back.
type Metrics15ShortcutPlanner struct {
	Function string
	Duration time.Duration
	Offset   time.Duration
	// Cells reads the cells of [From, To): fingerprint, c (cell start) and
	// cnt (line count).
	Cells shared.SQLRequestPlanner
	// Edges reads the lines on the window edges: fingerprint, ts and k (line
	// count).
	Edges shared.SQLRequestPlanner
}

// NewMetrics15sCells reads the metrics_15s cells, or the plugin's
// replacement.
func NewMetrics15sCells(function string, duration time.Duration) shared.SQLRequestPlanner {
	if p := plugins.GetMetrics15ShortcutPlannerPlugin(); p != nil {
		return (*p)(function, duration)
	}
	return &metrics15sCells{}
}

func (m *Metrics15ShortcutPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	if ctx.Grid == nil {
		return nil, errNoGrid
	}
	fn, ok := lineFn(m.Function, m.Duration)
	if !ok || fn.summand != "1" {
		return nil, &shared.NotSupportedError{Msg: m.Function + " is not supported by metrics_15s"}
	}
	g := *ctx.Grid
	rn, on := m.Duration.Nanoseconds(), m.Offset.Nanoseconds()
	lo, hi := g.FirstNs-rn-on, g.LastNs()-on

	from, to := ctx.From, ctx.To
	defer func() { ctx.From, ctx.To = from, to }()
	ctx.From, ctx.To = time.Unix(0, lo), time.Unix(0, hi)
	cells, err := m.Cells.Process(ctx)
	if err != nil {
		return nil, err
	}
	edges, err := m.Edges.Process(ctx)
	if err != nil {
		return nil, err
	}
	cellsW, edgesW := sql.NewWith(cells, "win_m"), sql.NewWith(edges, "win_x")
	moved := sql.NewCustomCol(func(c *sql.Ctx, opts ...int) (string, error) {
		cellsRef, err := sql.NewWithRef(cellsW).String(c, opts...)
		if err != nil {
			return "", err
		}
		edgesRef, err := sql.NewWithRef(edgesW).String(c, opts...)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("(SELECT fingerprint, c, cnt FROM %s UNION ALL "+
			"SELECT fingerprint, e.1, e.2 FROM %s ARRAY JOIN [(ts - %d, k), (ts, -k)] AS e)", cellsRef, edgesRef, cellNs), nil
	})
	sums := func(keys ...sql.SQLObject) sql.ISelect {
		cols := append([]sql.SQLObject{sql.NewRawObject("fingerprint")}, keys...)
		return sql.NewSelect().With(cellsW, edgesW).
			Select(append(cols,
				sql.NewSimpleCol("toFloat64(sum(cnt))", "s"),
				sql.NewSimpleCol("toInt64(sum(cnt))", "n"))...).
			From(sql.NewCol(moved, "win_c")).
			AndWhere(
				sql.Ge(sql.NewRawObject("c"), sql.NewIntVal(lo)),
				sql.Lt(sql.NewRawObject("c"), sql.NewIntVal(hi)))
	}

	w := window{grid: g, fn: fn, rn: rn}
	if g.StepNs == 0 {
		win := sql.NewWith(sums().
			GroupBy(sql.NewRawObject("fingerprint")).
			AndHaving(sql.Gt(sql.NewRawObject("n"), sql.NewIntVal(0))), "win_t")
		return w.emit(win, fmt.Sprintf("toInt64(%d)", g.FirstNs),
			fmt.Sprintf("any(%s)", fn.final("s", "n"))), nil
	}
	width := shared.Gcd(g.StepNs, rn)
	buckets := sql.NewWith(sums(sql.NewSimpleCol(fmt.Sprintf("intDiv(c + %d, %d) * %[2]d + %[2]d", on, width), "b")).
		GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("b")), "win_b")
	return w.cumulative(buckets), nil
}

// metrics15sCells reads the line count of every metrics_15s cell in
// [From, To).
type metrics15sCells struct{}

func (*metrics15sCells) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	return sql.NewSelect().
		Select(
			sql.NewSimpleCol("samples.fingerprint", "fingerprint"),
			sql.NewSimpleCol("samples.timestamp_ns", "c"),
			sql.NewSimpleCol("toInt64(countMerge(samples.count))", "cnt")).
		From(sql.NewSimpleCol(ctx.Metrics15sDistTableName, "samples")).
		AndWhere(
			sql.Ge(sql.NewRawObject("samples.timestamp_ns"), sql.NewIntVal(ctx.From.UnixNano())),
			sql.Lt(sql.NewRawObject("samples.timestamp_ns"), sql.NewIntVal(ctx.To.UnixNano())),
			GetTypes(ctx)).
		GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("c")), nil
}

// EdgeLinesPlanner counts the raw lines exactly on T-offset and T-offset-R
// for every point T of the grid.
type EdgeLinesPlanner struct {
	Duration time.Duration
	Offset   time.Duration
}

func (e *EdgeLinesPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	if ctx.Grid == nil {
		return nil, errNoGrid
	}
	g := *ctx.Grid
	rn, on := e.Duration.Nanoseconds(), e.Offset.Nanoseconds()
	first, last := g.FirstNs-on, g.LastNs()-on
	set := fmt.Sprintf("%d, %d", first, first-rn)
	if g.StepNs != 0 {
		set = fmt.Sprintf("SELECT arrayJoin(arrayConcat(range(%d, %d, %d), range(%d, %d, %[3]d)))",
			first, last+1, g.StepNs, first-rn, last-rn+1)
	}
	return sql.NewSelect().
		Select(
			sql.NewSimpleCol("samples.fingerprint", "fingerprint"),
			sql.NewSimpleCol("samples.timestamp_ns", "ts"),
			sql.NewSimpleCol("toInt64(count())", "k")).
		From(sql.NewSimpleCol(ctx.SamplesDistTableName, "samples")).
		AndPreWhere(
			sql.Ge(sql.NewRawObject("samples.timestamp_ns"), sql.NewIntVal(first-rn)),
			sql.Le(sql.NewRawObject("samples.timestamp_ns"), sql.NewIntVal(last)),
			sql.NewIn(sql.NewRawObject("samples.timestamp_ns"), sql.NewRawObject(set)),
			GetTypes(ctx)).
		GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("ts")), nil
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
