package clickhouse_transpiler

import (
	"fmt"
	"strings"

	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type ComplexOrPlanner struct {
	Operands []shared.SQLRequestPlanner
	Prefix   string
}

func (c ComplexOrPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	selects := make([]sql.ISelect, len(c.Operands))
	var err error
	for i, op := range c.Operands {
		selects[i], err = op.Process(ctx)
		if err != nil {
			return nil, err
		}
		selects[i].Select(
			append(selects[i].GetSelect(),
				sql.NewSimpleCol("max(timestamp_ns)", "max_timestamp_ns"))...)
		with := sql.NewWith(selects[i], fmt.Sprintf("_%d_pre_", i))
		selects[i] = sql.NewSelect().
			With(with).
			Select(sql.NewSimpleCol("trace_id", "trace_id"),
				sql.NewSimpleCol("_span_id", "span_id"),
				sql.NewSimpleCol("max_timestamp_ns", "max_timestamp_ns")).
			From(sql.NewWithRef(with)).
			Join(sql.NewJoin("array", sql.NewSimpleCol(with.GetAlias()+".span_id", "_span_id"), nil))
	}

	return sql.NewSelect().
		Select(sql.NewSimpleCol("trace_id", "trace_id"),
			sql.NewSimpleCol("groupUniqArray(100)(span_id)", "span_id")).
		From(sql.NewCol(&union{
			selects: selects,
		}, c.Prefix+"a")).
		GroupBy(sql.NewRawObject("trace_id")).
		OrderBy(sql.NewOrderBy(sql.NewRawObject("max(max_timestamp_ns)"), sql.ORDER_BY_DIRECTION_DESC)), nil
}

type union struct {
	selects []sql.ISelect
}

func (i *union) String(ctx *sql.Ctx, opts ...int) (string, error) {
	var _opts []int
	for _, opt := range opts {
		if opt != sql.STRING_OPT_SKIP_WITH {
			_opts = append(_opts, opt)
		}
	}
	strSelects := make([]string, len(i.selects))
	var err error
	for i, s := range i.selects {
		strSelects[i], err = s.String(ctx, _opts...)
		if err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("(%s)", strings.Join(strSelects, " UNION ALL ")), nil
}
