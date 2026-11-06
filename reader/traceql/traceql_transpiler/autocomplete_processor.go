package traceql_transpiler

import (
	"slices"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// autocompleteRequestProcessor reads the tags or values main selects.
type autocompleteRequestProcessor struct {
	main shared.SQLRequestPlanner
}

func (a *autocompleteRequestProcessor) Process(ctx *shared.PlannerContext) (chan []string, error) {
	res, err := readSuggestions(ctx, a.main)
	if err != nil {
		return nil, err
	}
	return sendSuggestions(res), nil
}

func (a *autocompleteRequestProcessor) SetMain(main shared.SQLRequestPlanner) {
	a.main = main
}

// complexAutocompleteRequestProcessor reads main one complexity portion of
// traces at a time and merges the portions' suggestions.
type complexAutocompleteRequestProcessor struct {
	main shared.SQLRequestPlanner
}

func (c *complexAutocompleteRequestProcessor) Process(ctx *shared.PlannerContext,
	portions int64) (chan []string, error) {
	parts := make([][]string, 0, portions)
	for i := range portions {
		portionCtx := *ctx
		portionCtx.RandomFilter = shared.RandomFilter{Max: int(portions), I: int(i)}
		portionCtx.CachedTraceIds = nil
		res, err := readSuggestions(&portionCtx, c.main)
		if err != nil {
			return nil, err
		}
		parts = append(parts, res)
	}
	return sendSuggestions(mergeSuggestions(parts, ctx.Limit)), nil
}

func (c *complexAutocompleteRequestProcessor) SetMain(main shared.SQLRequestPlanner) {
	c.main = main
}

// readSuggestions runs main and reads its one string column.
func readSuggestions(ctx *shared.PlannerContext, main shared.SQLRequestPlanner) ([]string, error) {
	req, err := main.Process(ctx)
	if err != nil {
		return nil, err
	}
	strReq, err := req.String(sql_select.DefaultCtx())
	if err != nil {
		return nil, err
	}
	rows, err := ctx.CHDb.QueryCtx(ctx.Ctx, strReq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		res = append(res, s)
	}
	if err := rows.Err(); err != nil {
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
