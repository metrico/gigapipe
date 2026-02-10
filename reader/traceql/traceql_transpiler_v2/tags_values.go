package traceql_transpiler_v2

import (
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v4/reader/traceql/tempo"
	"github.com/metrico/qryn/v4/reader/utils/logger"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

// PlanTagsV2 creates a planner for getting tag names from traces.
// If root is nil, returns all tags. If root is provided, returns tags from matching spans.
func PlanTagsV2(root *tempo.RootExpr) (shared.GenericTraceRequestProcessor[string], error) {
	if root == nil {
		// No filter - return all tags
		return &TagsRequestProcessor{sqlPlanner: &AllTagsPlanner{}}, nil
	}

	// Build the filter planner
	planner := &TempoPlanner{root: root}
	pipelinePlanner, err := planner.planPipeline(root.Pipeline)
	if err != nil {
		return nil, err
	}

	// Wrap with tags selection
	tagsPlanner := &FilteredTagsPlanner{Main: pipelinePlanner}

	return &TagsRequestProcessor{sqlPlanner: tagsPlanner}, nil
}

// PlanValuesV2 creates a planner for getting tag values from traces.
// If root is nil, returns all values for the key. If root is provided, returns values from matching spans.
func PlanValuesV2(root *tempo.RootExpr, key string) (shared.GenericTraceRequestProcessor[string], error) {
	if root == nil {
		// No filter - return all values for key
		return &TagsRequestProcessor{sqlPlanner: &AllValuesPlanner{Key: key}}, nil
	}

	// Build the filter planner
	planner := &TempoPlanner{root: root}
	pipelinePlanner, err := planner.planPipeline(root.Pipeline)
	if err != nil {
		return nil, err
	}

	// Wrap with values selection
	valuesPlanner := &FilteredValuesPlanner{Main: pipelinePlanner, Key: key}

	return &TagsRequestProcessor{sqlPlanner: valuesPlanner}, nil
}

// TagsRequestProcessor processes SQL queries and returns string results (tags or values).
type TagsRequestProcessor struct {
	sqlPlanner shared.SQLRequestPlanner
}

// Process executes the query and returns string results.
func (r *TagsRequestProcessor) Process(ctx *shared.PlannerContext) (chan []string, error) {
	sqlReq, err := r.sqlPlanner.Process(ctx)
	if err != nil {
		return nil, err
	}

	strReq, err := sqlReq.String(&sql.Ctx{
		Params: map[string]sql.SQLObject{},
		Result: map[string]sql.SQLObject{},
	})
	if err != nil {
		return nil, err
	}

	rows, err := ctx.CHDb.QueryCtx(ctx.Ctx, strReq)
	if err != nil {
		return nil, err
	}

	res := make(chan []string)

	go func() {
		defer rows.Close()
		defer close(res)

		for rows.Next() {
			var value string
			err = rows.Scan(&value)
			if err != nil {
				logger.Error("ERROR[TRP_V2_TAGS#1]: ", err)
				return
			}
			res <- []string{value}
		}
	}()

	return res, nil
}

// AllTagsPlanner returns all unique tag keys.
type AllTagsPlanner struct{}

func (a *AllTagsPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	table := ctx.TracesKVTable
	if ctx.IsCluster {
		table = ctx.TracesKVDistTable
	}

	query := sql.NewSelect().
		Distinct(true).
		Select(sql.NewSimpleCol("key", "key")).
		From(sql.NewRawObject(table)).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(ctx.From.Format("2006-01-02"))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(ctx.To.Format("2006-01-02"))),
		)

	if ctx.Limit > 0 {
		query = query.OrderBy(sql.NewOrderBy(sql.NewRawObject("key"), sql.ORDER_BY_DIRECTION_ASC)).
			Limit(sql.NewIntVal(ctx.Limit))
	}

	return query, nil
}

// AllValuesPlanner returns all unique values for a specific key.
type AllValuesPlanner struct {
	Key string
}

func (a *AllValuesPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	table := ctx.TracesKVTable
	if ctx.IsCluster {
		table = ctx.TracesKVDistTable
	}

	query := sql.NewSelect().
		Distinct(true).
		Select(sql.NewSimpleCol("val", "val")).
		From(sql.NewRawObject(table)).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(ctx.From.Format("2006-01-02"))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(ctx.To.Format("2006-01-02"))),
			sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(a.Key)),
		)

	if ctx.Limit > 0 {
		query = query.OrderBy(sql.NewOrderBy(sql.NewRawObject("val"), sql.ORDER_BY_DIRECTION_ASC)).
			Limit(sql.NewIntVal(ctx.Limit))
	}

	return query, nil
}

// FilteredTagsPlanner returns tag keys from spans matching the filter.
type FilteredTagsPlanner struct {
	Main shared.SQLRequestPlanner
}

func (f *FilteredTagsPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := f.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	table := ctx.TracesAttrsTable
	if ctx.IsCluster {
		table = ctx.TracesAttrsDistTable
	}

	// Create CTE for matching spans
	withMain := sql.NewWith(main, "matching_spans")
	preSelectSpans := sql.NewSelect().Select(sql.NewRawObject("span_id")).From(sql.NewWithRef(withMain))
	withPreSelectSpans := sql.NewWith(preSelectSpans, "pre_select_spans")

	query := sql.NewSelect().
		With(withMain, withPreSelectSpans).
		Distinct(true).
		Select(sql.NewSimpleCol("key", "key")).
		From(sql.NewSimpleCol(table, "traces_idx")).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(ctx.From.Format("2006-01-02"))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(ctx.To.Format("2006-01-02"))),
			sql.Ge(sql.NewRawObject("traces_idx.timestamp_ns"), sql.NewIntVal(ctx.From.UnixNano())),
			sql.Lt(sql.NewRawObject("traces_idx.timestamp_ns"), sql.NewIntVal(ctx.To.UnixNano())),
			sql.NewIn(sql.NewRawObject("span_id"), sql.NewWithRef(withPreSelectSpans)),
		)

	if ctx.Limit > 0 {
		query = query.OrderBy(sql.NewOrderBy(sql.NewRawObject("key"), sql.ORDER_BY_DIRECTION_ASC)).
			Limit(sql.NewIntVal(ctx.Limit))
	}

	return query, nil
}

// FilteredValuesPlanner returns tag values from spans matching the filter.
type FilteredValuesPlanner struct {
	Main shared.SQLRequestPlanner
	Key  string
}

func (f *FilteredValuesPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := f.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	table := ctx.TracesAttrsTable
	if ctx.IsCluster {
		table = ctx.TracesAttrsDistTable
	}

	// Create CTE for matching spans
	withMain := sql.NewWith(main, "matching_spans")
	preSelectSpans := sql.NewSelect().Select(sql.NewRawObject("span_id")).From(sql.NewWithRef(withMain))
	withPreSelectSpans := sql.NewWith(preSelectSpans, "pre_select_spans")

	query := sql.NewSelect().
		With(withMain, withPreSelectSpans).
		Distinct(true).
		Select(sql.NewSimpleCol("val", "val")).
		From(sql.NewSimpleCol(table, "traces_idx")).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(ctx.From.Format("2006-01-02"))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(ctx.To.Format("2006-01-02"))),
			sql.Ge(sql.NewRawObject("traces_idx.timestamp_ns"), sql.NewIntVal(ctx.From.UnixNano())),
			sql.Lt(sql.NewRawObject("traces_idx.timestamp_ns"), sql.NewIntVal(ctx.To.UnixNano())),
			sql.NewIn(sql.NewRawObject("span_id"), sql.NewWithRef(withPreSelectSpans)),
			sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(f.Key)),
		)

	if ctx.Limit > 0 {
		query = query.OrderBy(sql.NewOrderBy(sql.NewRawObject("val"), sql.ORDER_BY_DIRECTION_ASC)).
			Limit(sql.NewIntVal(ctx.Limit))
	}

	return query, nil
}

// Helper to get TracesKVTable with fallback
func getTracesKVTable(ctx *shared.PlannerContext) string {
	if ctx.TracesKVTable != "" {
		return ctx.TracesKVTable
	}
	return "tempo_traces_kv"
}

// Helper to get TracesKVDistTable with fallback
func getTracesKVDistTable(ctx *shared.PlannerContext) string {
	if ctx.TracesKVDistTable != "" {
		return ctx.TracesKVDistTable
	}
	return "tempo_traces_kv_dist"
}
