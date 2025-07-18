package clickhouse_planner

import (
	"fmt"

	"github.com/metrico/qryn/reader/logql/transpiler/shared"
	parser "github.com/metrico/qryn/reader/logql/parser"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type LabelFormatPlanner struct {
	Main shared.SQLRequestPlanner
	Expr *parser.LabelFormat

	formatters []*LineFormatPlanner
}

func (s *LabelFormatPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := s.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	err = s.makeFormatters()
	if err != nil {
		return nil, err
	}

	keys := make([]sql.SQLObject, len(s.Expr.LabelFormatOps))
	for i, o := range s.Expr.LabelFormatOps {
		keys[i] = sql.NewStringVal(o.Label.Name)
	}

	vals := make([]sql.SQLObject, len(s.Expr.LabelFormatOps))
	for i, o := range s.Expr.LabelFormatOps {
		if s.formatters[i] != nil {
			err = s.formatters[i].ProcessTpl(ctx)
			if err != nil {
				return nil, err
			}
			vals[i] = &sqlFormat{
				format: s.formatters[i].formatStr,
				args:   s.formatters[i].args,
			}
			continue
		}

		vals[i] = sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
			lbl, err := sql.NewStringVal(o.LabelVal.Name).String(ctx, options...)
			return fmt.Sprintf("labels[%s]", lbl), err
		})
	}

	cols, err := patchCol(main.GetSelect(), "labels", func(object sql.SQLObject) (sql.SQLObject, error) {
		return &sqlMapUpdate{
			m1: object,
			m2: &sqlMapInit{
				TypeName: "Map(String, String)",
				Keys:     keys,
				Values:   vals,
			},
		}, nil
	})

	return main.Select(cols...), err
}

func (s *LabelFormatPlanner) IsSupported() bool {
	err := s.makeFormatters()
	if err != nil {
		return false
	}

	for _, f := range s.formatters {
		if f != nil && !f.IsSupported() {
			return false
		}
	}
	return true
}

func (s *LabelFormatPlanner) makeFormatters() error {
	if s.formatters != nil {
		return nil
	}
	s.formatters = make([]*LineFormatPlanner, len(s.Expr.LabelFormatOps))
	for i, op := range s.Expr.LabelFormatOps {
		if op.ConstVal == nil {
			continue
		}

		val, err := op.ConstVal.Unquote()
		if err != nil {
			return err
		}

		s.formatters[i] = &LineFormatPlanner{Template: val}
	}
	return nil
}
