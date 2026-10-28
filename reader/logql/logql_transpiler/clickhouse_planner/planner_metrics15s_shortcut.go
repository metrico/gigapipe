package clickhouse_planner

import (
	"fmt"
	"math"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/plugins"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// cellNs is the width of a metrics_15s cell; cell c holds the lines in
// [c, c+15s).
const cellNs = int64(15 * time.Second)

// startLines says where the shortcut takes the lines exactly on a cell start
// from: the cells' start_count from cutoff on, the raw edge read before it.
type startLines struct {
	cutoff int64
}

// noStartLines reads no cell's start_count.
var noStartLines = startLines{cutoff: math.MaxInt64}

// fromCells reports whether any cell's start_count is read.
func (s startLines) fromCells() bool { return s != noStartLines }

// startLinesUpTo reads the start_count of the cells up to hi from the cutoff
// in the version info on.
func startLinesUpTo(ctx *shared.PlannerContext, hi int64) startLines {
	if from := ctx.VersionInfo.Metrics15sStartCountFrom(); from <= hi {
		return startLines{cutoff: from}
	}
	return noStartLines
}

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

// Metrics15ShortcutPlanner evaluates rate or count_over_time over
// (T-R-o, T-o] (o the offset) from the metrics_15s cells of [T-R-o, T-o),
// each raw line exactly on T-o or T-R-o moved one cell back.
type Metrics15ShortcutPlanner struct {
	Function string
	Duration time.Duration
	Offset   time.Duration
	// Cells reads the cells of [From, To): fingerprint, c (cell start) and
	// cnt (line count).
	Cells shared.SQLRequestPlanner
	// CellsByBucket: Cells already sums each stream's cells per window bucket
	// and carries their start lines (metrics15sCells does), so its rows need
	// no regrouping per stream.
	CellsByBucket bool
	// Edges reads the lines on the window edges: fingerprint, ts and k (line
	// count).
	Edges shared.SQLRequestPlanner
	// Fingerprints reads the streams with cells in [From, To]. It bounds the
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

	starts := noStartLines
	if m.CellsByBucket {
		starts = startLinesUpTo(ctx, hi)
	}

	from, to := ctx.From, ctx.To
	defer func() { ctx.From, ctx.To = from, to }()
	ctx.From, ctx.To = time.Unix(0, lo), time.Unix(0, hi)
	cells, err := m.Cells.Process(ctx)
	if err != nil {
		return nil, err
	}
	// Every edge is at or after lo; the cells carry those from the cutoff on.
	withEdges := lo < starts.cutoff
	var movedW *sql.With
	if withEdges {
		edges, err := m.Edges.Process(ctx)
		if err != nil {
			return nil, err
		}
		movedW = sql.NewWith(movedEdges(sql.NewWith(edges, "win_e")), "win_em")
	}
	cellsW := sql.NewWith(cells, "win_m")
	inRange := func(sel sql.ISelect) sql.ISelect {
		return sel.AndWhere(
			sql.Ge(sql.NewRawObject("c"), sql.NewIntVal(lo)),
			sql.Lt(sql.NewRawObject("c"), sql.NewIntVal(hi)))
	}
	// Cell hi is read too for its start lines, which count in the cell before.
	cellsInRange := inRange
	if starts.fromCells() {
		cellsInRange = func(sel sql.ISelect) sql.ISelect {
			return sel.AndWhere(
				sql.Ge(sql.NewRawObject("c"), sql.NewIntVal(lo)),
				sql.Le(sql.NewRawObject("c"), sql.NewIntVal(hi)))
		}
	}
	// A range grid's rows carry the start lines per bucket; an instant query
	// folds them into its one window: lo's fall out of it and hi's into it.
	bucketStarts := starts
	cnt := "toInt64(cnt)"
	if g.StepNs == 0 {
		bucketStarts = noStartLines
		if starts.fromCells() {
			cnt = fmt.Sprintf("toInt64(if(c < %d, cnt, 0) + if(c > %d, starts, 0))", hi, lo)
		}
	}
	// The cells and the moved edge lines, one row each.
	cellCols := []sql.SQLObject{sql.NewRawObject("fingerprint"),
		sql.NewSimpleCol("toInt64(c)", "c"),
		sql.NewSimpleCol(cnt, "cnt")}
	edgeCols := []sql.SQLObject{sql.NewRawObject("fingerprint"), sql.NewRawObject("c"), sql.NewRawObject("cnt")}
	if bucketStarts.fromCells() {
		cellCols = append(cellCols, sql.NewSimpleCol("toInt64(starts)", "starts"))
		edgeCols = append(edgeCols, sql.NewSimpleCol("toInt64(0)", "starts"))
	}
	cellRows := cellsInRange(sql.NewSelect().With(cellsW).Select(cellCols...).From(sql.NewWithRef(cellsW)))
	cellsAndEdgesW := sql.NewWith(cellRows, "win_c")
	if withEdges {
		cellsAndEdgesW = sql.NewWith(&UnionAll{
			ISelect: cellRows.AddWith(movedW),
			Anothers: []sql.ISelect{inRange(sql.NewSelect().
				Select(edgeCols...).
				From(sql.NewWithRef(movedW)))},
		}, "win_c")
	}
	if m.Group != nil {
		fps, err := m.Fingerprints.Process(ctx)
		if err != nil {
			return nil, err
		}
		group, err := m.Group.mapping(ctx, fps)
		if err != nil {
			return nil, err
		}
		return groupedWindows(g, fn, rn, on, cellsAndEdgesW, sql.NewWith(group, "win_g"), bucketStarts), nil
	}

	sums := func(keys ...sql.SQLObject) sql.ISelect {
		cols := append([]sql.SQLObject{sql.NewRawObject("fingerprint")}, keys...)
		return sql.NewSelect().With(cellsAndEdgesW).
			Select(append(cols,
				sql.NewSimpleCol("toFloat64(sum(cnt))", "s"),
				sql.NewSimpleCol("toInt64(sum(cnt))", "n"))...).
			From(sql.NewWithRef(cellsAndEdgesW))
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
	key := sql.NewSimpleCol(bucketKey("c", on, width), "b")
	if !m.CellsByBucket {
		return slideWindows(g, fn, rn, width, sql.NewWith(sums(key).
			GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("b")), "win_b"), false, noStartLines), nil
	}
	// The cells hold one row per stream and bucket already; the edges, if
	// any, summed per stream and bucket, are joined onto them.
	bcCols := []sql.SQLObject{sql.NewRawObject("fingerprint"), key, sql.NewSimpleCol("cnt", "n")}
	if starts.fromCells() {
		bcCols = append(bcCols, sql.NewRawObject("starts"))
	}
	cellBucketRows := cellsInRange(sql.NewSelect().With(cellsW).
		Select(bcCols...).
		From(sql.NewWithRef(cellsW)))
	if !withEdges {
		return slideWindows(g, fn, rn, width, sql.NewWith(cellBucketRows, "win_b"), false, starts), nil
	}
	cellBuckets := sql.NewWith(cellBucketRows, "win_bc")
	edgeBuckets := sql.NewWith(inRange(sql.NewSelect().With(movedW).
		Select(sql.NewRawObject("fingerprint"), key, sql.NewSimpleCol("toInt64(sum(cnt))", "n")).
		From(sql.NewWithRef(movedW))).
		GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("b")), "win_be")
	// With join_use_nulls=0 a bucket missing on one side reads b = 0 and n = 0
	// there; no bucket ends at 0.
	joinCols := []sql.SQLObject{
		sql.NewSimpleCol("if(win_bc.b = 0, win_be.fingerprint, win_bc.fingerprint)", "fingerprint"),
		sql.NewSimpleCol("if(win_bc.b = 0, win_be.b, win_bc.b)", "b"),
		sql.NewSimpleCol("win_bc.n + win_be.n", "n")}
	if starts.fromCells() {
		joinCols = append(joinCols, sql.NewSimpleCol("win_bc.starts", "starts"))
	}
	joined := sql.NewSelect().With(cellBuckets, edgeBuckets).
		Select(joinCols...).
		From(sql.NewCol(sql.NewWithRef(cellBuckets), "win_bc")).
		Join(sql.NewJoin("FULL", sql.NewCol(sql.NewWithRef(edgeBuckets), "win_be"), sql.And(
			sql.Eq(sql.NewRawObject("win_bc.fingerprint"), sql.NewRawObject("win_be.fingerprint")),
			sql.Eq(sql.NewRawObject("win_bc.b"), sql.NewRawObject("win_be.b"))))).
		SetSetting("join_use_nulls", "0")
	return slideWindows(g, fn, rn, width, sql.NewWith(joined, "win_b"), false, starts), nil
}

// bucketKey is the end of the w-wide bucket, in offset time, holding cell c.
func bucketKey(c string, on, width int64) string {
	return fmt.Sprintf("intDiv(%s + %d, %d) * %[3]d + %[3]d", c, on, width)
}

// movedEdges selects fingerprint, c and cnt for the edge lines of edgesW:
// each line counts in the cell before its own and is taken out of its own.
func movedEdges(edgesW *sql.With) sql.ISelect {
	return sql.NewSelect().With(edgesW).
		Select(sql.NewRawObject("fingerprint"), sql.NewSimpleCol("e.1", "c"), sql.NewSimpleCol("e.2", "cnt")).
		From(sql.NewWithRef(edgesW)).
		Join(sql.NewJoin("array", sql.NewSimpleCol(fmt.Sprintf("[(ts - %d, k), (ts, -k)]", cellNs), "e"), nil))
}

// slideWindows evaluates the windows of the grid over buckets, one row per
// fingerprint (a stream or a group) and bucket with its line count n: every
// fingerprint's empty buckets of the read range, first-R to last, are filled
// in, and the window at T is the sum of the R/w buckets ending at T. With
// labels, each fingerprint keeps the labels of its non-empty buckets.
//
// Start lines from the cells come per bucket as starts, which count in the
// bucket before: the window at T takes those of (T-R+w, T+w].
func slideWindows(g shared.EvalGrid, fn windowFn, rn, width int64, buckets *sql.With, labels bool,
	starts startLines) sql.ISelect {
	cols := []sql.SQLObject{sql.NewRawObject("fingerprint"), sql.NewRawObject("b"), sql.NewRawObject("n")}
	fillTo := g.LastNs() + width
	if starts.fromCells() {
		cols = append(cols, sql.NewRawObject("starts"))
		fillTo += width
	}
	if labels {
		cols = append(cols, sql.NewRawObject("labels"))
	}
	filled := sql.NewWith(sql.NewSelect().With(buckets).
		Select(cols...).
		From(sql.NewWithRef(buckets)).
		OrderBy(sql.NewRawObject("fingerprint"), sql.NewRawObject(fmt.Sprintf("b WITH FILL FROM %d TO %d STEP %d",
			g.FirstNs-rn+width, fillTo, width))).
		// Fill each fingerprint's buckets on its own. With this off, which a
		// server profile may do, WITH FILL fills one sequence across all of
		// them and the windows below sum the wrong rows.
		SetSetting("use_with_fill_by_sorting_prefix", "1"), "win_f")
	sum := fmt.Sprintf("sum(n) OVER (PARTITION BY fingerprint ORDER BY b "+
		"ROWS BETWEEN %d PRECEDING AND CURRENT ROW)", rn/width-1)
	nonEmpty := "n > 0"
	if starts.fromCells() {
		frameStart := "1 FOLLOWING"
		if k := rn / width; k == 2 {
			frameStart = "CURRENT ROW"
		} else if k > 2 {
			frameStart = fmt.Sprintf("%d PRECEDING", k-2)
		}
		sum += fmt.Sprintf(" + sum(starts) OVER (PARTITION BY fingerprint ORDER BY b ROWS BETWEEN %s AND 1 FOLLOWING)",
			frameStart)
		nonEmpty = "n > 0 OR starts > 0"
	}
	winCols := []sql.SQLObject{
		sql.NewRawObject("fingerprint"),
		sql.NewSimpleCol("b", "T"),
		sql.NewSimpleCol(sum, "s"),
	}
	resCols := []sql.SQLObject{
		sql.NewSimpleCol("T", "timestamp_ns"),
		sql.NewRawObject("fingerprint"),
		sql.NewSimpleCol(`''`, "string"),
		sql.NewSimpleCol(fn.final("toFloat64(s)", "s"), "value"),
	}
	if labels {
		winCols = append(winCols, sql.NewSimpleCol("anyIf(labels, "+nonEmpty+") OVER (PARTITION BY fingerprint)", "labels"))
		resCols = append(resCols, sql.NewRawObject("labels"))
	}
	windows := sql.NewWith(sql.NewSelect().With(filled).Select(winCols...).From(sql.NewWithRef(filled)), "win_s")
	return sql.NewSelect().With(windows).
		Select(resCols...).
		From(sql.NewWithRef(windows)).
		AndWhere(
			sql.Gt(sql.NewRawObject("s"), sql.NewIntVal(0)),
			sql.Ge(sql.NewRawObject("T"), sql.NewIntVal(g.FirstNs)),
			sql.Le(sql.NewRawObject("T"), sql.NewIntVal(g.LastNs())),
			sql.Eq(sql.NewRawObject(fmt.Sprintf("(T - %d) %% %d", g.FirstNs, g.StepNs)), sql.NewIntVal(0)))
}

// groupedWindows sums the rows (fingerprint, c, cnt) of rowsW per group and
// window bucket, then slides the windows over them (one bucket, R wide, for an
// instant query). Buckets are w = gcd(step, R) wide and keyed in offset time:
// bucket b holds the rows with c + o in [b-w, b), and the window at T the
// buckets of (T-R, T].
//
// With start lines from the cells, rowsW also carries starts.
func groupedWindows(g shared.EvalGrid, fn windowFn, rn, on int64, rowsW, groupW *sql.With,
	starts startLines) sql.ISelect {
	width := rn
	if g.StepNs != 0 {
		width = shared.Gcd(g.StepNs, rn)
	}
	bgCols := []sql.SQLObject{
		sql.NewSimpleCol("win_gm.new_fingerprint", "gfp"),
		sql.NewSimpleCol("win_gm.labels", "glabels"),
		sql.NewSimpleCol(bucketKey("win_mc.c", on, width), "b"),
		sql.NewSimpleCol("win_mc.cnt", "cnt")}
	bCols := []sql.SQLObject{
		sql.NewSimpleCol("gfp", "fingerprint"),
		sql.NewSimpleCol("b", "b"),
		sql.NewSimpleCol("toInt64(sum(cnt))", "n"),
		sql.NewSimpleCol("any(glabels)", "labels")}
	if starts.fromCells() {
		bgCols = append(bgCols, sql.NewSimpleCol("win_mc.starts", "bstarts"))
		bCols = append(bCols, sql.NewSimpleCol("toInt64(sum(bstarts))", "starts"))
	}
	byGroup := sql.NewWith(sql.NewSelect().With(rowsW, groupW).
		Select(bgCols...).
		From(sql.NewCol(sql.NewWithRef(rowsW), "win_mc")).
		Join(sql.NewJoin("INNER", sql.NewCol(sql.NewWithRef(groupW), "win_gm"),
			sql.Eq(sql.NewRawObject("win_mc.fingerprint"), sql.NewRawObject("win_gm.fingerprint")))), "win_bg")
	buckets := sql.NewWith(sql.NewSelect().With(byGroup).
		Select(bCols...).
		From(sql.NewWithRef(byGroup)).
		GroupBy(sql.NewRawObject("gfp"), sql.NewRawObject("b")), "win_b")

	if g.StepNs != 0 {
		return slideWindows(g, fn, rn, width, buckets, true, starts)
	}
	return sql.NewSelect().With(buckets).
		Select(
			sql.NewSimpleCol(fmt.Sprintf("toInt64(%d)", g.FirstNs), "timestamp_ns"),
			sql.NewRawObject("fingerprint"),
			sql.NewSimpleCol(`''`, "string"),
			sql.NewSimpleCol(fn.final("toFloat64(sum(n))", "sum(n)"), "value"),
			sql.NewSimpleCol("any(labels)", "labels")).
		From(sql.NewWithRef(buckets)).
		GroupBy(sql.NewRawObject("fingerprint")).
		AndHaving(sql.Gt(sql.NewRawObject("sum(n)"), sql.NewIntVal(0)))
}

// metrics15sCells reads the line count of the metrics_15s cells in
// [From, To). On a range grid it sums the cells of each window bucket into
// one row keyed on the bucket's first cell, which the shortcut buckets as it
// would the cell; From and To are bucket edges, so the bounds hold too.
//
// From the start_count cutoff on, a row also carries starts, the start lines
// of its first cell, out of cnt; cell To is then read too, for its starts.
type metrics15sCells struct {
	duration time.Duration
	offset   time.Duration
}

func (m *metrics15sCells) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	key, firstCell := "samples.timestamp_ns", ""
	if ctx.Grid != nil && ctx.Grid.StepNs != 0 {
		width, on := shared.Gcd(ctx.Grid.StepNs, m.duration.Nanoseconds()), m.offset.Nanoseconds()
		key = fmt.Sprintf("intDiv(samples.timestamp_ns + %d, %d) * %[2]d - %[1]d", on, width)
		firstCell = fmt.Sprintf(" AND (samples.timestamp_ns + %d) %% %d = 0", on, width)
	}
	to := ctx.To.UnixNano()
	cnt := "toInt64(countMerge(samples.count))"
	upTo := sql.Lt(sql.NewRawObject("samples.timestamp_ns"), sql.NewIntVal(to))
	var startsCols []sql.SQLObject
	if starts := startLinesUpTo(ctx, to); starts.fromCells() {
		startsExpr := fmt.Sprintf("toInt64(sumIf(samples.start_count, samples.timestamp_ns >= %d%s))",
			starts.cutoff, firstCell)
		cnt += " - " + startsExpr
		startsCols = append(startsCols, sql.NewSimpleCol(startsExpr, "starts"))
		upTo = sql.Le(sql.NewRawObject("samples.timestamp_ns"), sql.NewIntVal(to))
	}
	cols := append([]sql.SQLObject{
		sql.NewSimpleCol("samples.fingerprint", "fingerprint"),
		sql.NewSimpleCol(key, "c"),
		sql.NewSimpleCol(cnt, "cnt")}, startsCols...)
	return sql.NewSelect().
		Select(cols...).
		From(sql.NewSimpleCol(ctx.Metrics15sDistTableName, "samples")).
		AndWhere(
			sql.Ge(sql.NewRawObject("samples.timestamp_ns"), sql.NewIntVal(ctx.From.UnixNano())),
			upTo,
			GetTypes(ctx)).
		GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("c")), nil
}

// Metrics15sFingerprints reads the streams with metrics_15s cells in
// [first-R-offset, last-offset] of the request's grid.
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
			sql.Le(sql.NewRawObject("samples.timestamp_ns"), sql.NewIntVal(ctx.Grid.LastNs()-on)),
			GetTypes(ctx)), nil
}

// EdgeLinesPlanner counts the raw lines exactly on T-offset and T-offset-R
// for every point T of the grid.
type EdgeLinesPlanner struct {
	Duration time.Duration
	Offset   time.Duration
	// StartsFromCells: the cells carry the start lines from the start_count
	// cutoff on, so only the edges before it are read.
	StartsFromCells bool
}

func (e *EdgeLinesPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	if ctx.Grid == nil {
		return nil, errNoGrid
	}
	g := *ctx.Grid
	rn, on := e.Duration.Nanoseconds(), e.Offset.Nanoseconds()
	first, last := g.FirstNs-on, g.LastNs()-on
	// end bounds the edges read, exclusive.
	end := last + 1
	if e.StartsFromCells {
		end = min(end, startLinesUpTo(ctx, last).cutoff)
	}
	set := fmt.Sprintf("%d, %d", first, first-rn)
	switch {
	case g.StepNs != 0:
		set = fmt.Sprintf("SELECT arrayJoin(arrayConcat(range(%d, %d, %d), range(%d, %d, %[3]d)))",
			first, max(first, end), g.StepNs, first-rn, max(first-rn, min(end, last-rn+1)))
	case first >= end:
		set = fmt.Sprintf("%d", first-rn)
	}
	return sql.NewSelect().
		Select(
			sql.NewSimpleCol("samples.fingerprint", "fingerprint"),
			sql.NewSimpleCol("samples.timestamp_ns", "ts"),
			sql.NewSimpleCol("toInt64(count())", "k")).
		From(sql.NewSimpleCol(ctx.SamplesDistTableName, "samples")).
		AndPreWhere(
			sql.Ge(sql.NewRawObject("samples.timestamp_ns"), sql.NewIntVal(first-rn)),
			sql.Le(sql.NewRawObject("samples.timestamp_ns"), sql.NewIntVal(end-1)),
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
