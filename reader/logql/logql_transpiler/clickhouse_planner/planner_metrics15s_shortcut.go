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

// Metrics15ShortcutPlanner evaluates rate or count_over_time from whole
// metrics_15s cells: the window at T counts the lines in [T-R-o, T-o) (o the
// offset), so a line exactly on T-o falls in the next window. The raw route
// keeps (T-R-o, T-o].
type Metrics15ShortcutPlanner struct {
	Function string
	Duration time.Duration
	Offset   time.Duration
	// Cells reads the cells of [From, To): fingerprint, c (cell start) and
	// cnt (line count).
	Cells shared.SQLRequestPlanner
	// Fingerprints reads the streams with cells in [From, To). It bounds the
	// label lookup to them (#702) without reading the window again.
	Fingerprints shared.SQLRequestPlanner
	// Group, when set, sums the streams of each by/without group before the
	// window: the window of a sum of counts is the sum of their windows, and
	// it holds a row per group and point instead of per stream and point.
	Group *ShortcutGroup
}

// ShortcutGroup maps every stream to its by/without group as ByWithoutPlanner
// does: the group's fingerprint and its filtered labels.
type ShortcutGroup struct {
	Labels  []string
	By      bool
	FpCache **sql.With
}

// mapping selects fingerprint, new_fingerprint and labels, one row per stream
// that fps selects.
func (g *ShortcutGroup) mapping(ctx *shared.PlannerContext, fps sql.ISelect) (sql.ISelect, error) {
	var fpCache *sql.With
	if g.FpCache != nil {
		fpCache = *g.FpCache
	}
	ts, err := labelsFromScratch(ctx, fpCache, nil)
	if err != nil {
		return nil, err
	}
	boundTimeSeriesToFingerprints(ctx, ts, fps)
	var filtered sql.SQLObject
	cols, err := patchCol(ts.GetSelect(), "labels", func(labels sql.SQLObject) (sql.SQLObject, error) {
		filtered = &byWithoutFilterCol{labelsCol: labels, labels: g.Labels, by: g.By}
		return filtered, nil
	})
	if err != nil {
		return nil, err
	}
	// time_series holds a row per stream and date; the group is computed per
	// row so that only it, not the whole label set, is kept per stream.
	perRow := sql.NewWith(ts.Select(append(cols, sql.NewCol(&canonicalFPCol{inner: filtered}, "new_fingerprint"))...),
		"win_gr")
	return sql.NewSelect().With(perRow).
		Select(
			sql.NewRawObject("fingerprint"),
			sql.NewSimpleCol("any(new_fingerprint)", "new_fingerprint"),
			sql.NewSimpleCol("any(labels)", "labels")).
		From(sql.NewWithRef(perRow)).
		GroupBy(sql.NewRawObject("fingerprint")), nil
}

// NewMetrics15sCells reads the metrics_15s cells, or the plugin's
// replacement.
func NewMetrics15sCells(function string, duration, offset time.Duration) shared.SQLRequestPlanner {
	if p := plugins.GetMetrics15ShortcutPlannerPlugin(); p != nil {
		return (*p)(function, duration)
	}
	return &metrics15sCells{duration: duration, offset: offset}
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
	cellsW := sql.NewWith(cells, "win_m")
	if m.Group != nil {
		fps, err := m.Fingerprints.Process(ctx)
		if err != nil {
			return nil, err
		}
		group, err := m.Group.mapping(ctx, fps)
		if err != nil {
			return nil, err
		}
		return groupedWindows(g, fn, rn, on, lo, hi, cellsW, sql.NewWith(group, "win_g")), nil
	}

	sums := func(keys ...sql.SQLObject) sql.ISelect {
		cols := append([]sql.SQLObject{sql.NewRawObject("fingerprint")}, keys...)
		return sql.NewSelect().With(cellsW).
			Select(append(cols,
				sql.NewSimpleCol("toFloat64(sum(cnt))", "s"),
				sql.NewSimpleCol("toInt64(sum(cnt))", "n"))...).
			From(sql.NewWithRef(cellsW)).
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
	return streamWindows(g, fn, rn, on, sums), nil
}

// streamWindows evaluates every stream's windows with one window function:
// the cells summed per stream and bucket (w = gcd(step, R), keyed in offset
// time, bucket b holding c + o in [b-w, b)), every stream's empty buckets
// filled in, and the window at T the sum of the R/w buckets ending at T.
func streamWindows(g shared.EvalGrid, fn windowFn, rn, on int64, sums func(keys ...sql.SQLObject) sql.ISelect) sql.ISelect {
	width := shared.Gcd(g.StepNs, rn)
	buckets := sql.NewWith(sums(sql.NewSimpleCol(fmt.Sprintf("intDiv(c + %d, %d) * %[2]d + %[2]d", on, width), "b")).
		GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("b")), "win_b")
	// The buckets of the read range, first-R to last, for every stream.
	filled := sql.NewWith(sql.NewSelect().With(buckets).
		Select(sql.NewRawObject("fingerprint"), sql.NewRawObject("b"), sql.NewRawObject("n")).
		From(sql.NewWithRef(buckets)).
		OrderBy(sql.NewRawObject("fingerprint"), sql.NewRawObject(fmt.Sprintf("b WITH FILL FROM %d TO %d STEP %d",
			g.FirstNs-rn+width, g.LastNs()+width, width))), "win_f")
	windows := sql.NewWith(sql.NewSelect().With(filled).
		Select(
			sql.NewRawObject("fingerprint"),
			sql.NewSimpleCol("b", "T"),
			sql.NewSimpleCol(fmt.Sprintf("sum(n) OVER (PARTITION BY fingerprint ORDER BY b "+
				"ROWS BETWEEN %d PRECEDING AND CURRENT ROW)", rn/width-1), "s")).
		From(sql.NewWithRef(filled)), "win_s")
	return sql.NewSelect().With(windows).
		Select(
			sql.NewSimpleCol("T", "timestamp_ns"),
			sql.NewRawObject("fingerprint"),
			sql.NewSimpleCol(`''`, "string"),
			sql.NewSimpleCol(fn.final("toFloat64(s)", "s"), "value")).
		From(sql.NewWithRef(windows)).
		AndWhere(
			sql.Gt(sql.NewRawObject("s"), sql.NewIntVal(0)),
			sql.Ge(sql.NewRawObject("T"), sql.NewIntVal(g.FirstNs)),
			sql.Le(sql.NewRawObject("T"), sql.NewIntVal(g.LastNs())),
			sql.Eq(sql.NewRawObject(fmt.Sprintf("(T - %d) %% %d", g.FirstNs, g.StepNs)), sql.NewIntVal(0)))
}

// groupedWindows sums the cells of [lo, hi) per group and window bucket, then
// adds every bucket to the points of the grid whose window holds it.
// Buckets are w = gcd(step, R) wide and keyed in offset time: bucket b holds
// the cells with c + o in [b-w, b), and the window at T the buckets of
// (T-R, T].
func groupedWindows(g shared.EvalGrid, fn windowFn, rn, on, lo, hi int64, cellsW, groupW *sql.With) sql.ISelect {
	width := rn
	if g.StepNs != 0 {
		width = shared.Gcd(g.StepNs, rn)
	}
	byGroup := sql.NewCustomCol(func(c *sql.Ctx, opts ...int) (string, error) {
		cellsRef, err := sql.NewWithRef(cellsW).String(c, opts...)
		if err != nil {
			return "", err
		}
		groupRef, err := sql.NewWithRef(groupW).String(c, opts...)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("(SELECT win_gm.new_fingerprint AS gfp, win_gm.labels AS glabels, "+
			"intDiv(win_mc.c + %d, %d) * %[2]d + %[2]d AS b, win_mc.cnt AS cnt "+
			"FROM %s AS win_mc INNER JOIN %s AS win_gm ON win_mc.fingerprint = win_gm.fingerprint "+
			"WHERE win_mc.c >= %d AND win_mc.c < %d)", on, width, cellsRef, groupRef, lo, hi), nil
	})
	buckets := sql.NewWith(sql.NewSelect().With(cellsW, groupW).
		Select(
			sql.NewSimpleCol("gfp", "fingerprint"),
			sql.NewSimpleCol("b", "b"),
			sql.NewSimpleCol("toInt64(sum(cnt))", "n"),
			sql.NewSimpleCol("any(glabels)", "labels")).
		From(sql.NewCol(byGroup, "win_bg")).
		GroupBy(sql.NewRawObject("gfp"), sql.NewRawObject("b")), "win_b")

	value := fn.final("toFloat64(sum(n))", "sum(n)")
	res := sql.NewSelect().With(buckets).
		Select(
			sql.NewSimpleCol("T", "timestamp_ns"),
			sql.NewRawObject("fingerprint"),
			sql.NewSimpleCol(`''`, "string"),
			sql.NewSimpleCol(value, "value"),
			sql.NewSimpleCol("any(labels)", "labels")).
		From(sql.NewWithRef(buckets)).
		GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("T")).
		AndHaving(sql.Gt(sql.NewRawObject("sum(n)"), sql.NewIntVal(0)))
	if g.StepNs == 0 {
		return res.Join(sql.NewJoin("array", sql.NewSimpleCol(fmt.Sprintf("[toInt64(%d)]", g.FirstNs), "T"), nil))
	}
	// A bucket b feeds the points T of the grid with b <= T < b + R.
	points := fmt.Sprintf("range(toInt64(intDiv(b + %d, %d) * %[2]d), toInt64(b + %d), %[2]d)", g.StepNs-1, g.StepNs, rn)
	return res.Join(sql.NewJoin("array", sql.NewSimpleCol(points, "T"), nil)).
		AndWhere(
			sql.Ge(sql.NewRawObject("T"), sql.NewIntVal(g.FirstNs)),
			sql.Le(sql.NewRawObject("T"), sql.NewIntVal(g.LastNs())))
}

// metrics15sCells reads the line count of the metrics_15s cells in
// [From, To). On a range grid it sums the cells of each window bucket into
// one row keyed on the bucket's first cell, which the shortcut buckets as it
// would the cell; From and To are bucket edges, so the bounds hold too.
type metrics15sCells struct {
	duration time.Duration
	offset   time.Duration
}

func (m *metrics15sCells) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	key := "samples.timestamp_ns"
	if ctx.Grid != nil && ctx.Grid.StepNs != 0 {
		width, on := shared.Gcd(ctx.Grid.StepNs, m.duration.Nanoseconds()), m.offset.Nanoseconds()
		key = fmt.Sprintf("intDiv(samples.timestamp_ns + %d, %d) * %[2]d - %[1]d", on, width)
	}
	return sql.NewSelect().
		Select(
			sql.NewSimpleCol("samples.fingerprint", "fingerprint"),
			sql.NewSimpleCol(key, "c"),
			sql.NewSimpleCol("toInt64(countMerge(samples.count))", "cnt")).
		From(sql.NewSimpleCol(ctx.Metrics15sDistTableName, "samples")).
		AndWhere(
			sql.Ge(sql.NewRawObject("samples.timestamp_ns"), sql.NewIntVal(ctx.From.UnixNano())),
			sql.Lt(sql.NewRawObject("samples.timestamp_ns"), sql.NewIntVal(ctx.To.UnixNano())),
			GetTypes(ctx)).
		GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("c")), nil
}

// Metrics15sFingerprints reads the streams with metrics_15s cells in the read
// range of the request's grid, [first-R-offset, last-offset).
type Metrics15sFingerprints struct {
	Duration time.Duration
	Offset   time.Duration
}

func (f *Metrics15sFingerprints) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	if ctx.Grid == nil {
		return nil, errNoGrid
	}
	rn, on := f.Duration.Nanoseconds(), f.Offset.Nanoseconds()
	return sql.NewSelect().Distinct(true).
		Select(sql.NewSimpleCol("samples.fingerprint", "fingerprint")).
		From(sql.NewSimpleCol(ctx.Metrics15sDistTableName, "samples")).
		AndWhere(
			sql.Ge(sql.NewRawObject("samples.timestamp_ns"), sql.NewIntVal(ctx.Grid.FirstNs-rn-on)),
			sql.Lt(sql.NewRawObject("samples.timestamp_ns"), sql.NewIntVal(ctx.Grid.LastNs()-on)),
			GetTypes(ctx)), nil
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
