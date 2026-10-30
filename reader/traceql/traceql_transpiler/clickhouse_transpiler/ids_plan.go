package clickhouse_transpiler

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/ext"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// SelectionResultTable is the external table that carries compare()'s
// selection ids, with SearchResultColumns.
const SelectionResultTable = "_selection_result"

// IdsStatement lists (trace_id, span_id[]) rows, sent to the statement that
// follows it as the external table Table with SearchResultColumns.
type IdsStatement struct {
	Table     string
	Statement shared.SQLRequestPlanner
}

// NewIdsTable is an empty external table named name with SearchResultColumns.
func NewIdsTable(name string) (*ext.Table, error) {
	cols := make([]func(*ext.Table) error, len(SearchResultColumns))
	for i, c := range SearchResultColumns {
		cols[i] = ext.Column(c.Name, column.Type(c.Type))
	}
	return ext.NewTable(name, cols...)
}

// ReadIds runs each ids statement and returns its rows as its external table.
func ReadIds(ctx *shared.PlannerContext, statements []IdsStatement) ([]*ext.Table, error) {
	res := make([]*ext.Table, len(statements))
	for i, s := range statements {
		table, err := readIds(ctx, s)
		if err != nil {
			return nil, err
		}
		res[i] = table
	}
	return res, nil
}

func readIds(ctx *shared.PlannerContext, s IdsStatement) (*ext.Table, error) {
	table, err := NewIdsTable(s.Table)
	if err != nil {
		return nil, err
	}
	req, err := s.Statement.Process(ctx)
	if err != nil {
		return nil, err
	}
	str, err := req.String(sql.DefaultCtx())
	if err != nil {
		return nil, err
	}
	rows, err := ctx.CHDb.QueryCtx(ctx.Ctx, str)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			traceId string
			spanIds []string
		)
		if err := rows.Scan(&traceId, &spanIds); err != nil {
			return nil, err
		}
		if err := table.Append(traceId, spanIds); err != nil {
			return nil, err
		}
	}
	return table, rows.Err()
}

// spanIdsPlanner lists the span ids of Result, an evaluated pipeline, by
// trace. Sample, when set, is the sample of Result's operand reads.
type spanIdsPlanner struct {
	Result shared.SQLRequestPlanner
	Sample sql.SQLCondition
}

func (s *spanIdsPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := s.Result.Process(sampled(ctx, s.Sample))
	if err != nil {
		return nil, err
	}
	withMain := sql.NewWith(main, "result_spans")
	return sql.NewSelect().
		With(withMain).
		Select(sql.NewRawObject("trace_id"), sql.NewSimpleCol("groupArray(span_id)", "span_ids")).
		From(sql.NewWithRef(withMain)).
		GroupBy(sql.NewRawObject("trace_id")), nil
}

// windowPlanner processes Main over [From, To] instead of the query range.
type windowPlanner struct {
	Main     shared.SQLRequestPlanner
	From, To time.Time
}

func (w *windowPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	res := *ctx
	res.From, res.To = w.From, w.To
	return w.Main.Process(&res)
}

// idsTablePlanner reads the external ids table Table.
type idsTablePlanner struct {
	Table string
}

func (i idsTablePlanner) Process(*shared.PlannerContext) (sql.ISelect, error) {
	return readIdsTable(i.Table), nil
}

// readIdsTable selects SearchResultColumns from the ids table table.
func readIdsTable(table string) sql.ISelect {
	cols := make([]sql.SQLObject, len(SearchResultColumns))
	for i, c := range SearchResultColumns {
		cols[i] = sql.NewRawObject(c.Name)
	}
	return sql.NewSelect().Select(cols...).From(sql.NewRawObject(table))
}

// idsOf returns the CTEs over the ids table table: its rows, their trace ids
// and their (trace_id, span_id) pairs.
func idsOf(table, prefix string) (rows, traceIds, spanIds *sql.With) {
	rows = sql.NewWith(readIdsTable(table), prefix+"result_ids")
	traceIds, spanIds = idsIn(rows, prefix)
	return rows, traceIds, spanIds
}

// idsIn returns the trace ids and the (trace_id, span_id) pairs of rows, a
// CTE of (trace_id, span_id[]) rows.
func idsIn(rows *sql.With, prefix string) (traceIds, spanIds *sql.With) {
	traceIds = sql.NewWith(traceIdsOf(rows), prefix+"trace_ids")
	spanIds = sql.NewWith(
		sql.NewSelect().
			Select(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id")).
			From(sql.NewWithRef(rows)).
			Join(sql.NewJoin("array", sql.NewRawObject("span_id"), nil)),
		prefix+"trace_span_ids")
	return traceIds, spanIds
}
