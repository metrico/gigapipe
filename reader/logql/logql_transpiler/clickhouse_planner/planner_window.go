package clickhouse_planner

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// windowFn is a range aggregation evaluated over (T-R, T] at every point T of
// the evaluation grid. It is either summable or a mergeable State.
type windowFn struct {
	// summand makes the function summable: its value is final(s, n), with s
	// the window's sum of summand and n its row count.
	summand string
	final   func(s, n string) string
	// finite sums only finite summands and counts NaN, +Inf and -Inf apart.
	finite bool
	// agg is a mergeable aggregate such as "quantile" with its parameters,
	// e.g. "(0.9)", applied to args.
	agg    string
	params string
	args   string
}

func (f windowFn) summable() bool {
	return f.summand != ""
}

func perSecond(r time.Duration) func(s, n string) string {
	return func(s, _ string) string {
		return fmt.Sprintf("%s / %f", s, r.Seconds())
	}
}

func plainSum(s, _ string) string {
	return s
}

func mean(s, n string) string {
	return fmt.Sprintf("%s / %s", s, n)
}

// partial is a per-bucket sum that windows add up.
type partial struct {
	name string
	expr string
}

func (f windowFn) partials() []partial {
	if !f.finite {
		return []partial{
			{"s", fmt.Sprintf("toFloat64(sum(%s))", f.summand)},
			{"n", "toInt64(count())"},
		}
	}
	x := f.summand
	return []partial{
		{"s", fmt.Sprintf("toFloat64(sumIf(assumeNotNull(%s), isFinite(%[1]s)))", x)},
		{"n", "toInt64(count())"},
		{"n_nan", fmt.Sprintf("toInt64(countIf(isNaN(%s)))", x)},
		{"n_pinf", fmt.Sprintf("toInt64(countIf(%s = inf))", x)},
		{"n_ninf", fmt.Sprintf("toInt64(countIf(%s = -inf))", x)},
	}
}

// value is the window's value from the sums named by col.
func (f windowFn) value(col func(name string) string) string {
	v := f.final(col("s"), col("n"))
	if !f.finite {
		return v
	}
	return fmt.Sprintf("multiIf(%s > 0 OR (%s > 0 AND %s > 0), nan, %[2]s > 0, inf, %[3]s > 0, -inf, %s)",
		col("n_nan"), col("n_pinf"), col("n_ninf"), v)
}

func (f windowFn) direct() string {
	if f.summable() {
		exprs := map[string]string{}
		for _, p := range f.partials() {
			exprs[p.name] = p.expr
		}
		return f.value(func(name string) string { return exprs[name] })
	}
	return fmt.Sprintf("%s%s(%s)", f.agg, f.params, f.args)
}

var errNoGrid = errors.New("range aggregation without an evaluation grid")

// readWindowRows plans main over the rows the windows of ctx.Grid read,
// (first-R-offset, last-offset].
func readWindowRows(ctx *shared.PlannerContext, main shared.SQLRequestPlanner,
	r, offset time.Duration) (sql.ISelect, error) {
	if ctx.Grid == nil {
		return nil, errNoGrid
	}
	from, to := ctx.From, ctx.To
	defer func() { ctx.From, ctx.To = from, to }()
	ctx.From = time.Unix(0, ctx.Grid.FirstNs-r.Nanoseconds()-offset.Nanoseconds()+1)
	ctx.To = time.Unix(0, ctx.Grid.LastNs()-offset.Nanoseconds()+1)
	return main.Process(ctx)
}

// window evaluates fn over (T-R, T] per fingerprint at every point of grid.
type window struct {
	grid   shared.EvalGrid
	fn     windowFn
	rn     int64
	labels bool
}

// windowSelect evaluates fn on the rows of main, read by readWindowRows,
// whose timestamp_ns carries the offset. Buckets are w = gcd(step, R) wide
// and left-open, so every window is whole buckets.
func windowSelect(ctx *shared.PlannerContext, main sql.ISelect, fn windowFn,
	r time.Duration, withLabels bool) (sql.ISelect, error) {
	if ctx.Grid == nil {
		return nil, errNoGrid
	}
	w := window{grid: *ctx.Grid, fn: fn, rn: r.Nanoseconds(), labels: withLabels}
	in := sql.NewWith(main, "win_in")
	if w.grid.StepNs == 0 {
		return w.instant(in), nil
	}
	width := shared.Gcd(w.grid.StepNs, w.rn)
	cols := []sql.SQLObject{
		sql.NewRawObject("fingerprint"),
		sql.NewSimpleCol(fmt.Sprintf("intDiv(timestamp_ns - 1, %d) * %[1]d + %[1]d", width), "b"),
	}
	if fn.summable() {
		for _, p := range fn.partials() {
			cols = append(cols, sql.NewSimpleCol(p.expr, p.name))
		}
	} else {
		cols = append(cols, sql.NewSimpleCol(fmt.Sprintf("%sState%s(%s)", fn.agg, fn.params, fn.args), "st"))
	}
	buckets := sql.NewWith(sql.NewSelect().With(in).Select(w.withLabels(cols, "any(labels)")...).
		From(sql.NewWithRef(in)).
		GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("b")), "win_b")
	if fn.summable() {
		return w.cumulative(buckets), nil
	}
	return w.fanOut(buckets), nil
}

func (w window) withLabels(cols []sql.SQLObject, expr string) []sql.SQLObject {
	if !w.labels {
		return cols
	}
	return append(cols, sql.NewSimpleCol(expr, "labels"))
}

// emit selects one row per fingerprint and timestamp_ns from src.
func (w window) emit(src *sql.With, ts, value string) sql.ISelect {
	return sql.NewSelect().With(src).Select(w.withLabels([]sql.SQLObject{
		sql.NewSimpleCol(ts, "timestamp_ns"),
		sql.NewRawObject("fingerprint"),
		sql.NewSimpleCol(`''`, "string"),
		sql.NewSimpleCol(value, "value"),
	}, "any(labels)")...).
		From(sql.NewWithRef(src)).
		GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("timestamp_ns"))
}

// ceilStep rounds the ns expression x up to the grid's step.
func (w window) ceilStep(x string) string {
	return fmt.Sprintf("intDiv(%s + %d, %d) * %[3]d", x, w.grid.StepNs-1, w.grid.StepNs)
}

// cumulative gives each point cum(T) - cum(T-R): bucket b adds its sums at
// the first point >= b and takes them back at the first point >= b+R.
func (w window) cumulative(buckets *sql.With) sql.ISelect {
	ps := w.fn.partials()
	add, sub := make([]string, len(ps)), make([]string, len(ps))
	for i, p := range ps {
		add[i], sub[i] = p.name, "-"+p.name
	}
	events := fmt.Sprintf("[(%s, %s), (%s, %s)]",
		w.ceilStep("b"), strings.Join(add, ", "),
		w.ceilStep(fmt.Sprintf("b + %d", w.rn)), strings.Join(sub, ", "))
	evCols := []sql.SQLObject{
		sql.NewRawObject("fingerprint"),
		sql.NewSimpleCol("ev.1", "t"),
	}
	for i, p := range ps {
		evCols = append(evCols, sql.NewSimpleCol(fmt.Sprintf("sum(ev.%d)", i+2), "d_"+p.name))
	}
	ev := sql.NewWith(sql.NewSelect().With(buckets).Select(w.withLabels(evCols, "any(labels)")...).
		From(sql.NewWithRef(buckets)).
		Join(sql.NewJoin("array", sql.NewSimpleCol(events, "ev"), nil)).
		GroupBy(sql.NewRawObject("fingerprint"), sql.NewRawObject("t")), "win_e")

	const running = "OVER (PARTITION BY fingerprint ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
	end := w.grid.LastNs() + w.grid.StepNs
	runCols := []sql.SQLObject{
		sql.NewRawObject("fingerprint"),
		sql.NewRawObject("t"),
		sql.NewSimpleCol(fmt.Sprintf("leadInFrame(t, 1, toInt64(%d)) OVER "+
			"(PARTITION BY fingerprint ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)", end),
			"t_next"),
	}
	for _, p := range ps {
		runCols = append(runCols, sql.NewSimpleCol(fmt.Sprintf("sum(d_%s) %s", p.name, running), "c_"+p.name))
	}
	run := sql.NewWith(sql.NewSelect().With(ev).Select(w.withLabels(runCols, "labels")...).
		From(sql.NewWithRef(ev)), "win_r")

	val := w.fn.value(func(name string) string { return "c_" + name })
	res := w.emit(run,
		fmt.Sprintf("arrayJoin(range(greatest(t, toInt64(%d)), least(t_next, toInt64(%d)), %d))",
			w.grid.FirstNs, end, w.grid.StepNs),
		fmt.Sprintf("any(%s)", val))
	return res.AndWhere(sql.Gt(sql.NewRawObject("c_n"), sql.NewIntVal(0)))
}

// fanOut sends each bucket's State to every point T in [b, b+R) and merges
// them per point.
func (w window) fanOut(buckets *sql.With) sql.ISelect {
	return w.emit(buckets,
		fmt.Sprintf("arrayJoin(range(greatest(%s, toInt64(%d)), least(b + %d, toInt64(%d)), %d))",
			w.ceilStep("b"), w.grid.FirstNs, w.rn, w.grid.LastNs()+w.grid.StepNs, w.grid.StepNs),
		fmt.Sprintf("%sMerge%s(st)", w.fn.agg, w.fn.params))
}

// instant evaluates fn once over every row read, the window (T-R, T].
func (w window) instant(in *sql.With) sql.ISelect {
	cols := []sql.SQLObject{
		sql.NewRawObject("fingerprint"),
		sql.NewSimpleCol(w.fn.direct(), "v"),
	}
	win := sql.NewWith(sql.NewSelect().With(in).Select(w.withLabels(cols, "any(labels)")...).
		From(sql.NewWithRef(in)).
		GroupBy(sql.NewRawObject("fingerprint")), "win_t")
	return w.emit(win, fmt.Sprintf("toInt64(%d)", w.grid.FirstNs), "any(v)")
}
