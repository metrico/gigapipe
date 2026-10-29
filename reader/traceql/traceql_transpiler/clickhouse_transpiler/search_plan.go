package clickhouse_transpiler

import (
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// SearchResultTable is the external table that carries Search's ids to Data.
const SearchResultTable = "_search_result"

// SearchResultColumn is a column of SearchResultTable.
type SearchResultColumn struct {
	Name string
	Type string
}

// SearchResultColumns are SearchResultTable's columns: the first two columns
// of a Search row.
var SearchResultColumns = []SearchResultColumn{
	{Name: "trace_id", Type: "FixedString(16)"},
	{Name: "span_id", Type: "Array(FixedString(8))"},
}

// SearchPlan is a search in two statements: Search lists traces with their
// spansets, and Data reads the traces of the ids in SearchResultTable.
type SearchPlan struct {
	Search shared.SQLRequestPlanner
	Data   shared.SQLRequestPlanner
}

// searchRowsPlanner returns Main's (trace_id, span_id[], spansets) rows as
// Search rows.
type searchRowsPlanner struct {
	Main shared.SQLRequestPlanner
}

func (s *searchRowsPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := s.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	withMain := sql.NewWith(main, "search_spansets")
	return sql.NewSelect().
		With(withMain).
		Select(
			sql.NewRawObject("trace_id"),
			sql.NewRawObject("span_id"),
			sql.NewSimpleCol("arrayMap(s -> arrayMap(x -> lower(hex(x)), s.2), spansets)", "spanset_span_ids"),
			sql.NewSimpleCol("arrayMap(s -> s.3, spansets)", "matched"),
			sql.NewSimpleCol("arrayMap(s -> arrayMap(x -> x.1, s.4), spansets)", "attr_names"),
			sql.NewSimpleCol("arrayMap(s -> arrayMap(x -> x.2, s.4), spansets)", "attr_types"),
			sql.NewSimpleCol("arrayMap(s -> arrayMap(x -> x.3, s.4), spansets)", "attr_values"),
			sql.NewSimpleCol("arrayMap(s -> arrayMap(x -> x.4, s.4), spansets)", "attr_texts")).
		From(sql.NewWithRef(withMain)), nil
}

// searchResultPlanner reads SearchResultTable.
type searchResultPlanner struct{}

func (searchResultPlanner) Process(*shared.PlannerContext) (sql.ISelect, error) {
	cols := make([]sql.SQLObject, len(SearchResultColumns))
	for i, c := range SearchResultColumns {
		cols[i] = sql.NewRawObject(c.Name)
	}
	return sql.NewSelect().Select(cols...).From(sql.NewRawObject(SearchResultTable)), nil
}
