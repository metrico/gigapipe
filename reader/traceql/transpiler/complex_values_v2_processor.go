package transpiler

import (
	"github.com/metrico/qryn/reader/logql/transpiler/shared"
	"github.com/metrico/qryn/reader/traceql/transpiler/clickhouse_transpiler"
	"github.com/metrico/qryn/reader/utils/sql_select"
)

type allValuesV2RequestProcessor struct {
	key string
}

func (c *allValuesV2RequestProcessor) Process(ctx *shared.PlannerContext) (chan []string, error) {
	planner := &clickhouse_transpiler.AllValuesRequestPlanner{
		Key: c.key,
	}
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

type ComplexValuesV2RequestProcessor struct {
	allValuesV2RequestProcessor
}

func (c *ComplexValuesV2RequestProcessor) Process(ctx *shared.PlannerContext,
	complexity int64) (chan []string, error) {
	return c.allValuesV2RequestProcessor.Process(ctx)
}

func (c *ComplexValuesV2RequestProcessor) SetMain(main shared.SQLRequestPlanner) {
}
