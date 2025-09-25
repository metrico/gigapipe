package prof_transpiler

import (
	"fmt"

	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type ProfileSizePlanner struct {
	/* MergeProfilesPlanner */
	Main shared.SQLRequestPlanner
}

func (p *ProfileSizePlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := p.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	withMain := sql.NewWith(main, "pre_profile_size")
	selectProfileSize := sql.NewSelect().
		Select(sql.NewRawObject("sum(length(payload)::Int64)")).
		From(sql.NewWithRef(withMain))
	var withFP *sql.With
	for _, with := range main.GetWith() {
		if with.GetAlias() == "fp" {
			withFP = with
			break
		}
	}

	selectFPCount := sql.NewSelect().
		Select(sql.NewRawObject("uniqExact(fingerprint)::Int64")).
		From(sql.NewWithRef(withFP))

	brackets := func(o sql.SQLObject) sql.SQLObject {
		return sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
			str, err := o.String(ctx, options...)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("(%s)", str), nil
		})
	}

	res := sql.NewSelect().
		With(withMain).
		Select(sql.NewCol(brackets(selectProfileSize), "profile_size"),
			sql.NewCol(brackets(selectFPCount), "fingerprint_count"))
	return res, nil
}
