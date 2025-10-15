package clickhouse_planner

import (
	"fmt"

	"github.com/metrico/qryn/v4/reader/logql/logql_parser"
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

// Filter labels on time_series table if no parsers are
// applied before
type SimpleLabelFilterPlanner struct {
	Expr  *logql_parser.LabelFilter
	FPSel shared.SQLRequestPlanner
}

func (s *SimpleLabelFilterPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	//TODO: offset
	//TODO: add time from-to?
	main, err := s.FPSel.Process(ctx)
	if err != nil {
		return nil, err
	}

	id := fmt.Sprintf("subsel_%d", ctx.Id())
	withMain := sql.NewWith(main, id)
	filterPlanner := &LabelFilterPlanner{
		Expr: s.Expr,
		MainReq: sql.NewSelect().
			With(withMain).
			Select(sql.NewRawObject("fingerprint")).
			From(sql.NewRawObject(ctx.TimeSeriesTableName)).
			AndWhere(sql.NewIn(sql.NewRawObject("fingerprint"), sql.NewWithRef(withMain))),
		LabelValGetter: func(s string) sql.SQLObject {
			return sql.NewRawObject(fmt.Sprintf("JSONExtractString(labels, '%s')", s))
		},
	}
	return filterPlanner.Process(ctx)
}
