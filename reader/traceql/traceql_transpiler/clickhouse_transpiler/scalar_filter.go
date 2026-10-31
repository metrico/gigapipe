package clickhouse_transpiler

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// The error texts hold a literal "%v".
var (
	errScalarFilterLHS = errors.New("scalar filter lhs of type (%v) not yet supported")
	errScalarFilterRHS = errors.New("scalar filter rhs of type (%v) not yet supported")
)

// The types of a spanset attribute's value; a duration is in nanoseconds.
const (
	AttrInt      = "int"
	AttrFloat    = "float"
	AttrDuration = "duration"
	AttrBool     = "bool"
	AttrString   = "string"
	AttrNil      = "nil"
)

// scalarFilterStage keeps the spansets whose aggregate over their spans passes
// `agg op value` and appends the aggregate to their attributes. A spanset is
// the spans of a trace with one group key.
type scalarFilterStage struct {
	agg   *traceql_ir.Aggregate
	op    string
	value float64
	// duration is whether the aggregate's argument is a duration.
	duration bool
}

// newScalarFilterStage validates f: an aggregate over a number compared with
// a number or duration literal. Both sides compare as floats, durations in
// nanoseconds.
func newScalarFilterStage(f *traceql_ir.ScalarFilter) (*scalarFilterStage, error) {
	for _, side := range []traceql_ir.ScalarExpr{f.LHS, f.RHS} {
		if err := checkAggregates(side); err != nil {
			return nil, err
		}
	}
	agg, ok := f.LHS.(*traceql_ir.Aggregate)
	if !ok {
		return nil, errScalarFilterLHS
	}
	lit, ok := f.RHS.(*traceql_ir.Static)
	if !ok {
		return nil, errScalarFilterRHS
	}
	value, err := staticValue(lit)
	if err != nil {
		return nil, err
	}
	duration := agg.Arg != nil && traceql_ir.TypeOf(agg.Arg) == traceql_ir.TypeDuration
	return &scalarFilterStage{agg: agg, op: f.Op, value: value, duration: duration}, nil
}

// checkAggregates checks that every aggregate of e reads a number of the span.
func checkAggregates(e traceql_ir.ScalarExpr) error {
	switch e := e.(type) {
	case *traceql_ir.ScalarOp:
		if err := checkAggregates(e.LHS); err != nil {
			return err
		}
		return checkAggregates(e.RHS)
	case *traceql_ir.Aggregate:
		if err := traceql_ir.CheckAggregate(e); err != nil {
			return err
		}
		if e.Arg != nil {
			_, err := pivotOf(e.Arg)
			return err
		}
	}
	return nil
}

// staticValue is a literal as a float: a duration in nanoseconds.
func staticValue(lit *traceql_ir.Static) (float64, error) {
	if lit.Kind == traceql_ir.ValueDuration {
		d, err := time.ParseDuration(lit.Text)
		if err != nil {
			return 0, err
		}
		return float64(d.Nanoseconds()), nil
	}
	return strconv.ParseFloat(lit.Text, 64)
}

func (s *scalarFilterStage) process(ctx *shared.PlannerContext, alias string, prev, base *sql.With) (sql.ISelect, error) {
	var (
		withs  []*sql.With
		valued sql.ISelect
	)
	window := "OVER (PARTITION BY trace_id, grp)"
	if s.agg.Arg == nil {
		valued = sql.NewSelect().
			Select(append(spanRowCols(),
				sql.NewSimpleCol("attrs", "prev_attrs"),
				sql.NewSimpleCol("grp", "grp"),
				sql.NewSimpleCol("toFloat64(uniqExact(span_id) "+window+")", "value"),
				sql.NewSimpleCol("1", "n_present"),
				sql.NewSimpleCol(sqlString(AttrInt), "type"))...).
			From(sql.NewWithRef(prev))
	} else {
		sel, err := s.values(ctx, alias, base)
		if err != nil {
			return nil, err
		}
		values := sql.NewWith(sel, alias+"_values")
		withs = append(withs, values)
		p, v := prev.GetAlias(), values.GetAlias()
		window = fmt.Sprintf("OVER (PARTITION BY %s.trace_id, %s.grp)", p, p)
		value, typ := s.aggregate(v, window)
		valued = sql.NewSelect().
			Select(
				sql.NewSimpleCol(p+".trace_id", "trace_id"),
				sql.NewSimpleCol(p+".span_id", "span_id"),
				sql.NewSimpleCol(p+".timestamp_ns", "timestamp_ns"),
				sql.NewSimpleCol(p+".attrs", "prev_attrs"),
				sql.NewSimpleCol(p+".grp", "grp"),
				sql.NewSimpleCol(value, "value"),
				sql.NewSimpleCol(fmt.Sprintf("countIf(%s.present = 1) %s", v, window), "n_present"),
				sql.NewSimpleCol(typ, "type")).
			From(sql.NewWithRef(prev)).
			Join(sql.NewJoin("left", sql.NewWithRef(values), sql.And(
				sql.Eq(sql.NewRawObject(p+".trace_id"), sql.NewRawObject(v+".trace_id")),
				sql.Eq(sql.NewRawObject(p+".span_id"), sql.NewRawObject(v+".span_id")))))
	}
	withValued := sql.NewWith(valued, alias+"_valued")
	withs = append(withs, withValued)
	return sql.NewSelect().
		With(withs...).
		Select(stageRowCols("",
			fmt.Sprintf("arrayPushBack(prev_attrs, tuple(%s, type, value, ''))", sqlString(s.agg.String())),
			"grp")...).
		From(sql.NewWithRef(withValued)).
		AndWhere(rawCond("n_present > 0 AND NOT isNaN(value) AND " + s.comparison("value"))), nil
}

// values reads the argument of every span of base that has one: num, is_int
// (whether it is an integer) and present, 1; a span left out reads present 0
// through the left join. Base spans start in the query range, which bounds the
// read.
func (s *scalarFilterStage) values(ctx *shared.PlannerContext, alias string, base *sql.With) (sql.ISelect, error) {
	pivot, err := pivotOf(s.agg.Arg)
	if err != nil {
		return nil, err
	}
	num, err := pivot.sql().number(s.agg.Arg)
	if err != nil {
		return nil, err
	}
	baseSpans := sql.NewSelect().Select(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id")).From(sql.NewWithRef(base))
	spans := pivot.spans(ctx)
	table := "traces_idx"
	if len(pivot.keys) == 0 {
		table = "traces"
	}
	spans.AndWhere(inSubquery(ctx, sql.NewRawObject(fmt.Sprintf("(%s.trace_id, %s.span_id)", table, table)), baseSpans))
	withs, withSpans := pivot.read(ctx, spans, alias+"_pivot")
	return sql.NewSelect().
		With(withs...).
		Select(
			sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("span_id", "span_id"),
			sql.NewSimpleCol("ifNull("+num.val+", 0)", "num"),
			sql.NewSimpleCol(num.isInt, "is_int"),
			sql.NewSimpleCol("1", "present")).
		From(sql.NewWithRef(withSpans)).
		AndWhere(rawCond("isNotNull(" + num.val + ")")), nil
}

// aggregate returns the aggregate over the present values of v per spanset,
// and its type: a duration for a duration argument, else an int when every
// value it depends on is an integer. avg(duration) truncates to whole
// nanoseconds.
func (s *scalarFilterStage) aggregate(v, window string) (value string, typ string) {
	num, present, isInt := v+".num", v+".present = 1", v+".is_int"
	numeric := func(intExpr string) string {
		if s.duration {
			return sqlString(AttrDuration)
		}
		return fmt.Sprintf("if(%s %s = 1, %s, %s)", intExpr, window, sqlString(AttrInt), sqlString(AttrFloat))
	}
	switch s.agg.Fn {
	case "sum":
		return fmt.Sprintf("sumIf(%s, %s) %s", num, present, window), numeric(fmt.Sprintf("minIf(%s, %s)", isInt, present))
	case "min":
		return fmt.Sprintf("minIf(%s, %s) %s", num, present, window), numeric(fmt.Sprintf("argMinIf(%s, %s, %s)", isInt, num, present))
	case "max":
		return fmt.Sprintf("maxIf(%s, %s) %s", num, present, window), numeric(fmt.Sprintf("argMaxIf(%s, %s, %s)", isInt, num, present))
	}
	if s.duration {
		return fmt.Sprintf("toFloat64(intDivOrZero(sumIf(toInt64(%s), %s) %s, countIf(%s) %s))", num, present, window, present, window), sqlString(AttrDuration)
	}
	return fmt.Sprintf("avgIf(%s, %s) %s", num, present, window), sqlString(AttrFloat)
}

// comparison is `col Op Value`.
func (s *scalarFilterStage) comparison(col string) string {
	return fmt.Sprintf("%s %s %s", col, s.op, floatLiteral(s.value))
}

// floatLiteral renders f as a ClickHouse Float64.
func floatLiteral(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	}
	return "toFloat64(" + strconv.FormatFloat(f, 'g', -1, 64) + ")"
}

// sqlString renders s as a ClickHouse string literal.
func sqlString(s string) string {
	res, _ := sql.NewStringVal(s).String(nil)
	return res
}
