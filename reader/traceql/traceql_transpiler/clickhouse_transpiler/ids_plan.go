package clickhouse_transpiler

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/ext"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// idsColumns are the columns of an ids table: the first two columns of an
// ids statement's rows.
var idsColumns = []struct{ Name, Type string }{
	{Name: "trace_id", Type: "FixedString(16)"},
	{Name: "span_id", Type: "Array(FixedString(8))"},
}

// selectionResultTable is the external table that carries compare()'s
// selection ids, with idsColumns.
const selectionResultTable = "_selection_result"

// IdsStatement lists (trace_id, span_id[]) rows, sent to the statement that
// follows it as the external table Table with idsColumns.
type IdsStatement struct {
	Table     string
	Statement shared.SQLRequestPlanner
}

// newIdsTable is an empty external table named name with idsColumns.
func newIdsTable(name string) (*ext.Table, error) {
	cols := make([]func(*ext.Table) error, len(idsColumns))
	for i, c := range idsColumns {
		cols[i] = ext.Column(c.Name, column.Type(c.Type))
	}
	return ext.NewTable(name, cols...)
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

// readIdsTable selects idsColumns from the ids table table.
func readIdsTable(table string) sql.ISelect {
	cols := make([]sql.SQLObject, len(idsColumns))
	for i, c := range idsColumns {
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
