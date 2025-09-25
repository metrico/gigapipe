package prof_transpiler

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/reader/prof/prof_parser"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type MergeProfilesPlanner struct {
	Fingerprints shared.SQLRequestPlanner
	Selectors    []prof_parser.Selector
}

func (m *MergeProfilesPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	fp, err := m.Fingerprints.Process(ctx)
	if err != nil {
		return nil, err
	}

	matchers, err := (&StreamSelectorPlanner{Selectors: m.Selectors}).getMatchers()
	if err != nil {
		return nil, err
	}

	withFpSel := sql.NewWith(fp, "fp")
	main := sql.NewSelect().
		With(withFpSel).
		Select(sql.NewRawObject("payload")).
		From(sql.NewRawObject(ctx.ProfilesDistTable)).
		AndWhere(
			sql.Ge(sql.NewRawObject("timestamp_ns"), sql.NewIntVal(ctx.From.UnixNano())),
			sql.Le(sql.NewRawObject("timestamp_ns"), sql.NewIntVal(ctx.To.UnixNano())),
			sql.NewIn(sql.NewRawObject("fingerprint"), sql.NewWithRef(withFpSel)))
	if len(matchers.globalMatchers) > 0 {
		main.AndWhere(matchers.globalMatchers...)
	}
	if ctx.Limit != 0 {
		main.OrderBy(sql.NewOrderBy(sql.NewRawObject("timestamp_ns"), sql.ORDER_BY_DIRECTION_DESC)).
			Limit(sql.NewIntVal(ctx.Limit))
	}
	return main, nil
}
