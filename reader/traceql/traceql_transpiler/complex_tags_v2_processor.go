package traceql_transpiler

import (
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v4/reader/traceql/traceql_transpiler/clickhouse_transpiler"
	"github.com/metrico/qryn/v4/reader/utils/sql_select"
)

type allTagsV2RequestProcessor struct{}

func (c *allTagsV2RequestProcessor) Process(ctx *shared.PlannerContext) (chan []string, error) {
	planner := &clickhouse_transpiler.AllTagsRequestPlanner{}
	req, err := planner.Process(ctx)
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
	var tags []string
	for rows.Next() {
		var tag string
		err = rows.Scan(&tag)
		if err != nil {
			return nil, err
		}
		tags = append(tags, tag)
	}
	res := make(chan []string, 2)
	res <- tags
	go func() { close(res) }()
	return res, nil
}

type ComplexTagsV2RequestProcessor struct {
	allTagsV2RequestProcessor
}

func (c *ComplexTagsV2RequestProcessor) Process(ctx *shared.PlannerContext,
	complexity int64) (chan []string, error) {
	return c.allTagsV2RequestProcessor.Process(ctx)
}

func (c *ComplexTagsV2RequestProcessor) SetMain(main shared.SQLRequestPlanner) {
}
