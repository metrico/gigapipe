package traceql_transpiler

import (
	"database/sql"
	"slices"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
)

// autocompleteRequestProcessor reads the tags or values plan selects.
type autocompleteRequestProcessor struct {
	plan *clickhouse_transpiler.SearchPlan
}

func (a *autocompleteRequestProcessor) Process(ctx *shared.PlannerContext) (chan []string, error) {
	res, err := readSuggestions(ctx, a.plan)
	if err != nil {
		return nil, err
	}
	return sendSuggestions(res), nil
}

func (a *autocompleteRequestProcessor) SetMain(plan *clickhouse_transpiler.SearchPlan) {
	a.plan = plan
}

// complexAutocompleteRequestProcessor reads plan one complexity portion of
// traces at a time and merges the portions' suggestions.
type complexAutocompleteRequestProcessor struct {
	plan *clickhouse_transpiler.SearchPlan
}

func (c *complexAutocompleteRequestProcessor) Process(ctx *shared.PlannerContext,
	complexity int64) (chan []string, error) {
	portions := (complexity + COMPLEXITY_THRESHOLD - 1) / COMPLEXITY_THRESHOLD
	parts := make([][]string, 0, portions)
	for i := range portions {
		portionCtx := *ctx
		portionCtx.RandomFilter = shared.RandomFilter{Max: int(portions), I: int(i)}
		portionCtx.CachedTraceIds = nil
		res, err := readSuggestions(&portionCtx, c.plan)
		if err != nil {
			return nil, err
		}
		parts = append(parts, res)
	}
	return sendSuggestions(mergeSuggestions(parts, ctx.Limit)), nil
}

func (c *complexAutocompleteRequestProcessor) SetMain(plan *clickhouse_transpiler.SearchPlan) {
	c.plan = plan
}

// readSuggestions runs plan's Ids, then Data over them; Data runs alone when
// the plan has no ids statements, and not at all when they find no ids.
func readSuggestions(ctx *shared.PlannerContext, plan *clickhouse_transpiler.SearchPlan) ([]string, error) {
	var res []string
	err := clickhouse_transpiler.Query(ctx, plan.Ids, nil, plan.Data, clickhouse_transpiler.SkipOnEmptyIds,
		func(rows *sql.Rows) error {
			var s string
			if err := rows.Scan(&s); err != nil {
				return err
			}
			res = append(res, s)
			return nil
		})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// mergeSuggestions returns the distinct suggestions of parts in order, up to
// limit when it is positive.
func mergeSuggestions(parts [][]string, limit int64) []string {
	res := slices.Concat(parts...)
	slices.Sort(res)
	res = slices.Compact(res)
	if limit > 0 && int64(len(res)) > limit {
		res = res[:limit]
	}
	return res
}

func sendSuggestions(res []string) chan []string {
	ch := make(chan []string, 1)
	ch <- res
	close(ch)
	return ch
}
