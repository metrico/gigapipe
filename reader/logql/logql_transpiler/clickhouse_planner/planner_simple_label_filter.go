package clickhouse_planner

import (
	"fmt"

	"github.com/metrico/qryn/v4/reader/logql/logql_parser"
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

// Filter labels on time_series table if no parsers are
// applied before
// TODO: REVIEW, maybe outdated
type SimpleLabelFilterPlanner struct {
	NoStreamSelect bool
	Expr           *logql_parser.LabelFilter
	FPSel          shared.SQLRequestPlanner
}

func (s *SimpleLabelFilterPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	mainReq := sql.NewSelect().
		Select(sql.NewRawObject("fingerprint")).
		From(sql.NewRawObject(ctx.TimeSeriesTableName))

	if !s.NoStreamSelect {
		main, err := s.FPSel.Process(ctx)
		if err != nil {
			return nil, err
		}

		id := fmt.Sprintf("subsel_%d", ctx.Id())
		withMain := sql.NewWith(main, id)
		mainReq = mainReq.With(withMain).
			AndWhere(sql.NewIn(sql.NewRawObject("fingerprint"), sql.NewWithRef(withMain)))
	}

	//TODO: offset
	//TODO: add time from-to?
	filterPlanner := &LabelFilterPlanner{
		Expr:    s.Expr,
		MainReq: mainReq,
		LabelValGetter: func(s string) sql.SQLObject {
			return sql.NewRawObject(fmt.Sprintf("JSONExtractString(labels, '%s')", s))
		},
	}
	return filterPlanner.Process(ctx)
}
