package promql_transpiler

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type StreamSelectCombiner struct {
	Main           shared.SQLRequestPlanner
	StreamSelector shared.SQLRequestPlanner
}

func (s *StreamSelectCombiner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := s.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	fpQuery, err := s.StreamSelector.Process(ctx)
	if err != nil {
		return nil, err
	}
	withFpQuery := sql.NewWith(fpQuery, "fp_sel")
	main.AddWith(withFpQuery).
		AndWhere(sql.NewIn(sql.NewRawObject("fingerprint"), sql.NewWithRef(withFpQuery)))
	return main, nil
}
