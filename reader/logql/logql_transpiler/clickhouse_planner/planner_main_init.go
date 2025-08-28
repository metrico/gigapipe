package clickhouse_planner

import (
	"fmt"
	"time"

	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/reader/plugins"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type SqlMainInitPlanner struct {
	offset *time.Duration
}

func NewSQLMainInitPlanner(offset *time.Duration) shared.SQLRequestPlanner {
	p := plugins.GetSqlMainInitPlannerPlugin()
	if p != nil {
		return (*p)()
	}
	return &SqlMainInitPlanner{
		offset: offset,
	}
}

func (s *SqlMainInitPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	from := ctx.From
	to := ctx.To
	offsetNsStr := ""
	if s.offset != nil {
		from = from.Add(*s.offset)
		to = to.Add(*s.offset)
		offsetNsStr = fmt.Sprintf(" + %d", s.offset.Nanoseconds())
	}
	return sql.NewSelect().
		Select(
			sql.NewSimpleCol(fmt.Sprintf("samples.timestamp_ns%s", offsetNsStr), "timestamp_ns"),
			sql.NewSimpleCol("samples.fingerprint", "fingerprint"),
			sql.NewSimpleCol("samples.string", "string"),
			sql.NewSimpleCol("toFloat64(0)", "value"),
		).From(sql.NewSimpleCol(ctx.SamplesTableName, "samples")).
		AndPreWhere(
			sql.Ge(sql.NewRawObject("samples.timestamp_ns"), sql.NewIntVal(from.UnixNano())),
			sql.Lt(sql.NewRawObject("samples.timestamp_ns"), sql.NewIntVal(to.UnixNano())),
			GetTypes(ctx)), nil
}
