package clickhouse_planner

import (
	"github.com/metrico/qryn/reader/logql/transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type MainFinalizerPlanner struct {
	Main     shared.SQLRequestPlanner
	IsMatrix bool
	IsFinal  bool
	Alias    string
}

func (m *MainFinalizerPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	req, err := m.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	if !ctx.CHFinalize {
		return req, nil
	}

	if m.Alias == "" {
		m.Alias = "prefinal"
	}

	if m.IsMatrix {
		return m.processMatrix(ctx, req)
	}

	dir := sql.ORDER_BY_DIRECTION_DESC
	if ctx.OrderASC {
		dir = sql.ORDER_BY_DIRECTION_ASC
	}

	orderBy := []sql.SQLObject{
		sql.NewOrderBy(sql.NewRawObject("timestamp_ns"), dir),
	}
	if m.IsFinal {
		orderBy = []sql.SQLObject{
			sql.NewOrderBy(sql.NewRawObject("fingerprint"), dir),
			sql.NewOrderBy(sql.NewRawObject("timestamp_ns"), dir),
		}
	}

	withReq := sql.NewWith(req, m.Alias)
	return sql.NewSelect().
		With(withReq).
		Select(
			sql.NewSimpleCol(m.Alias+".fingerprint", "fingerprint"),
			sql.NewSimpleCol(m.Alias+".labels", "labels"),
			sql.NewSimpleCol(m.Alias+".string", "string"),
			sql.NewSimpleCol(m.Alias+".timestamp_ns", "timestamp_ns")).
		From(sql.NewWithRef(withReq)).
		OrderBy(orderBy...), nil
}

func (m *MainFinalizerPlanner) processMatrix(_ *shared.PlannerContext, req sql.ISelect) (sql.ISelect, error) {
	withReq := sql.NewWith(req, m.Alias)
	return sql.NewSelect().
		With(withReq).
		Select(
			sql.NewSimpleCol(m.Alias+".fingerprint", "fingerprint"),
			sql.NewSimpleCol(m.Alias+".labels", "labels"),
			sql.NewSimpleCol(m.Alias+".value", "value"),
			sql.NewSimpleCol(m.Alias+".timestamp_ns", "timestamp_ns")).
		From(sql.NewWithRef(withReq)).
		OrderBy(
			sql.NewOrderBy(sql.NewRawObject("fingerprint"), sql.ORDER_BY_DIRECTION_ASC),
			sql.NewOrderBy(sql.NewRawObject("timestamp_ns"), sql.ORDER_BY_DIRECTION_ASC)), nil
}
