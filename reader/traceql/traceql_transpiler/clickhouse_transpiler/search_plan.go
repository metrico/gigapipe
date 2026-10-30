package clickhouse_transpiler

import (
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// SearchResultTable is the ids table of a search or metrics query's result spans.
const SearchResultTable = "_search_result"

// SearchPlan is a read in two statements: Ids list ids, and Data reads the
// traces or attributes of those ids. Without Ids, Data reads alone.
type SearchPlan struct {
	Ids  []IdsStatement
	Data shared.SQLRequestPlanner
}

// searchRowsPlanner returns Main's rows as a search's ids rows: the ids, then
// each trace's spansets.
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
