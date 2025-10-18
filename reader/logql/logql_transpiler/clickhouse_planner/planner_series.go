package clickhouse_planner

import (
	"time"

	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/reader/plugins"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type SeriesPlanner struct {
	NoStreamSelect      bool //Not used
	FingerprintsPlanner shared.SQLRequestPlanner
	Offset              *time.Duration
}

func NewSeriesPlanner(fingerprintsPlanner shared.SQLRequestPlanner, offset *time.Duration) shared.SQLRequestPlanner {
	p := plugins.GetSeriesPlannerPlugin()
	if p != nil {
		return (*p)(fingerprintsPlanner)
	}
	return &SeriesPlanner{FingerprintsPlanner: fingerprintsPlanner}
}

func (s *SeriesPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	from := ctx.From
	to := ctx.To
	if s.Offset != nil {
		from = from.Add(*s.Offset)
		to = to.Add(*s.Offset)
	}

	req := sql.NewSelect().Distinct(true).
		Select(sql.NewSimpleCol("labels", "labels")).
		From(sql.NewSimpleCol(ctx.TimeSeriesDistTableName, "time_series")).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(FormatFromDate(from))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(to.Format("2006-01-02"))),
			GetTypes(ctx))
	if ctx.Limit > 0 {
		req.Limit(sql.NewIntVal(ctx.Limit))
	}
	if !s.NoStreamSelect {
		fpSel, err := s.FingerprintsPlanner.Process(ctx)
		if err != nil {
			return nil, err
		}
		withFPSel := sql.NewWith(fpSel, "fp_sel")
		req = req.With(withFPSel).AndWhere(sql.NewIn(sql.NewRawObject("fingerprint"), sql.NewWithRef(withFPSel)))
	}
	return req, nil
}
