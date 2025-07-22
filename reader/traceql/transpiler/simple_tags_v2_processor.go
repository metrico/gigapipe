package transpiler

import (
	"github.com/metrico/qryn/reader/logql/transpiler/shared"
	"github.com/metrico/qryn/reader/utils/sql_select"
)

type SimpleTagsV2RequestProcessor struct {
	main shared.SQLRequestPlanner
}

func (s *SimpleTagsV2RequestProcessor) Process(ctx *shared.PlannerContext) (chan []string, error) {
	req, err := s.main.Process(ctx)
	if err != nil {
		return nil, err
	}

	strReq, err := req.String(sql_select.DefaultCtx())
	if err != nil {
		return nil, err
	}
	println(strReq)

	rows, err := ctx.CHDb.QueryCtx(ctx.Ctx, strReq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []string
	for rows.Next() {
		var tag string
		err = rows.Scan(&tag)
		if err != nil {
			return nil, err
		}
		res = append(res, tag)
	}

	cRes := make(chan []string, 2)
	cRes <- res
	go func() { close(cRes) }()
	return cRes, nil
}

func (s *SimpleTagsV2RequestProcessor) SetMain(main shared.SQLRequestPlanner) {
	s.main = main
}
