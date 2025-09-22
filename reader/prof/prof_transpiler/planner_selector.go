package prof_transpiler

import (
	"fmt"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/clickhouse_planner"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	"github.com/metrico/qryn/reader/prof/prof_parser"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type StreamSelectorPlanner struct {
	Selectors []prof_parser.Selector
}

func (s *StreamSelectorPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	matchers, err := s.getMatchers()
	if err != nil {
		return nil, err
	}
	res := sql.NewSelect().
		Select(sql.NewRawObject("fingerprint")).
		From(sql.NewRawObject(ctx.ProfilesSeriesGinTable)).
		AndWhere(
			sql.Ge(sql.NewRawObject("date"), sql.NewStringVal(clickhouse_planner.FormatFromDate(ctx.From))),
			sql.Le(sql.NewRawObject("date"), sql.NewStringVal(clickhouse_planner.FormatFromDate(ctx.To)))).
		GroupBy(sql.NewRawObject("fingerprint"))
	if len(matchers.globalMatchers) > 0 {
		res = res.AndWhere(sql.And(matchers.globalMatchers...))
	}
	if len(matchers.kvMatchers) > 0 {
		res = res.
			AndWhere(sql.Or(matchers.kvMatchers...)).
			AndHaving(sql.Eq(
				clickhouse_planner.NewSqlBitSetAnd(matchers.kvMatchers),
				sql.NewIntVal((1<<len(matchers.kvMatchers))-1)))
	}
	return res, nil
}

type matchersResponse struct {
	globalMatchers []sql.SQLCondition
	kvMatchers     []sql.SQLCondition
}

func (s *StreamSelectorPlanner) getMatchers() (*matchersResponse, error) {
	var globalClauses []sql.SQLCondition
	var kvClauses []sql.SQLCondition
	for _, selector := range s.Selectors {
		_str, err := selector.Val.Unquote()
		if err != nil {
			return nil, err
		}
		var clause sql.SQLCondition
		switch selector.Name {
		case "__name__":
			clause, err = s.getMatcherClause(
				sql.NewRawObject("splitByChar(':', type_id)[1]"), selector.Op, sql.NewStringVal(_str))
			if err != nil {
				return nil, err
			}
		case "__period_type__":
			clause, err = s.getMatcherClause(
				sql.NewRawObject("splitByChar(':', type_id)[2]"), selector.Op, sql.NewStringVal(_str))
			if err != nil {
				return nil, err
			}
		case "__period_unit__":
			clause, err = s.getMatcherClause(
				sql.NewRawObject("splitByChar(':', type_id)[3]"), selector.Op, sql.NewStringVal(_str))
			if err != nil {
				return nil, err
			}
		case "__sample_type__":
			cond, err := s.getMatcherClause(
				sql.NewRawObject("x.1"),
				selector.Op,
				sql.NewStringVal(_str))
			if err != nil {
				return nil, err
			}
			clause = sql.Eq(s.getArrayExists(cond, sql.NewRawObject("sample_types_units")), sql.NewIntVal(1))
		case "__sample_unit__":
			cond, err := s.getMatcherClause(
				sql.NewRawObject("x.2"),
				selector.Op,
				sql.NewStringVal(_str))
			if err != nil {
				return nil, err
			}
			clause = sql.Eq(s.getArrayExists(cond, sql.NewRawObject("sample_types_units")), sql.NewIntVal(1))
		case "__profile_type__":
			fieldToMatch := "format('{}:{}:{}:{}:{}', (splitByChar(':', type_id) as _parts)[1], x.1, x.2, _parts[2], _parts[3])"
			cond, err := s.getMatcherClause(
				sql.NewRawObject(fieldToMatch),
				selector.Op,
				sql.NewStringVal(_str))
			if err != nil {
				return nil, err
			}
			clause = sql.Eq(s.getArrayExists(cond, sql.NewRawObject("sample_types_units")), sql.NewIntVal(1))
		case "service_name":
			clause, err = s.getMatcherClause(sql.NewRawObject("service_name"), selector.Op, sql.NewStringVal(_str))
			if err != nil {
				return nil, err
			}
		}
		if clause != nil {
			globalClauses = append(globalClauses, clause)
			continue
		}
		clause, err = s.getMatcherClause(sql.NewRawObject("val"), selector.Op, sql.NewStringVal(_str))
		if err != nil {
			return nil, err
		}
		clause = sql.And(sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(selector.Name)), clause)
		kvClauses = append(kvClauses, clause)
	}
	return &matchersResponse{
		globalMatchers: globalClauses,
		kvMatchers:     kvClauses,
	}, nil
}

func (s *StreamSelectorPlanner) getArrayExists(cond sql.SQLCondition, field sql.SQLObject) sql.SQLObject {
	return sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
		strCond, err := cond.String(ctx, options...)
		if err != nil {
			return "", err
		}
		strField, err := field.String(ctx, options...)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("arrayExists(x -> %s, %s)", strCond, strField), nil
	})
}

func (s *StreamSelectorPlanner) getMatcherClause(field sql.SQLObject, op string,
	val sql.SQLObject) (sql.SQLCondition, error) {
	switch op {
	case "=":
		return sql.Eq(field, val), nil
	case "!=":
		return sql.Neq(field, val), nil
	case "=~":
		return sql.Eq(sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
			strField, err := field.String(ctx, options...)
			if err != nil {
				return "", err
			}
			strVal, err := val.String(ctx, options...)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("match(%s, %s)", strField, strVal), nil
		}), sql.NewRawObject("1")), nil
	case "!~":
		return sql.Neq(sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
			strField, err := field.String(ctx, options...)
			if err != nil {
				return "", err
			}
			strVal, err := val.String(ctx, options...)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("match(%s, %s)", strField, strVal), nil
		}), sql.NewRawObject("1")), nil
	}
	return nil, fmt.Errorf("unknown operator: %s", op)
}
