package transpiler

import (
	"fmt"

	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/reader/prof/parser"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type MergeRawPlanner struct {
	Fingerprints shared.SQLRequestPlanner
	selectors    []parser.Selector
	sampleType   string
	sampleUnit   string
}

func (m *MergeRawPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	fpSel, err := m.Fingerprints.Process(ctx)
	if err != nil {
		return nil, err
	}
	matchers, err := (&StreamSelectorPlanner{Selectors: m.selectors}).getMatchers()
	if err != nil {
		return nil, err
	}
	withFpSel := sql.NewWith(fpSel, "fp")
	main := sql.NewSelect().
		With(withFpSel).
		Select(
			sql.NewCol(sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
				val := sql.NewStringVal(m.sampleType + ":" + m.sampleUnit)
				strVal, err := val.String(ctx, options...)
				if err != nil {
					return "", err
				}
				return fmt.Sprintf(
					"arrayMap(x -> (x.1, x.2, x.3, (arrayFirst(y -> y.1 == %s, x.4) as af).2, af.3), tree)",
					strVal), nil
			}), "tree"),
			sql.NewRawObject("functions")).
		From(sql.NewRawObject(ctx.ProfilesDistTable)).
		AndWhere(
			sql.Ge(sql.NewRawObject("timestamp_ns"), sql.NewIntVal(ctx.From.UnixNano())),
			sql.Lt(sql.NewRawObject("timestamp_ns"), sql.NewIntVal(ctx.To.UnixNano())),
			sql.NewIn(sql.NewRawObject("fingerprint"), sql.NewWithRef(withFpSel)),
			sql.And(matchers.globalMatchers...))
	if ctx.Limit != 0 {
		main.OrderBy(sql.NewOrderBy(sql.NewRawObject("timestamp_ns"), sql.ORDER_BY_DIRECTION_DESC)).
			Limit(sql.NewIntVal(ctx.Limit))
	}
	return main, nil
}
