package clickhouse_planner

import (
	"fmt"
	"strings"
	"time"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

type SqlMatch struct {
	col        sql.SQLObject
	pattern    string
	patternObj sql.SQLObject
}

func (s *SqlMatch) String(ctx *sql.Ctx, opts ...int) (string, error) {
	strCol, err := s.col.String(ctx, opts...)
	if err != nil {
		return "", err
	}

	if s.patternObj == nil {
		s.patternObj = sql.NewStringVal(s.pattern)
	}

	strVal, err := s.patternObj.String(ctx, opts...)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("match(%s, %s)", strCol, strVal), nil
}

type sqlMapUpdate struct {
	m1 sql.SQLObject
	m2 sql.SQLObject
}

func (s *sqlMapUpdate) String(ctx *sql.Ctx, opts ...int) (string, error) {
	str1, err := s.m1.String(ctx, opts...)
	if err != nil {
		return "", err
	}
	str2, err := s.m2.String(ctx, opts...)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("mapUpdate(%s, %s)", str1, str2), nil
}

func patchCol(cols []sql.SQLObject, name string,
	patch func(sql.SQLObject) (sql.SQLObject, error)) ([]sql.SQLObject, error) {
	_cols := make([]sql.SQLObject, len(cols))
	for i, c := range cols {
		_c, ok := c.(sql.Aliased)
		if !ok || _c.GetAlias() != name {
			_cols[i] = c
			continue
		}

		__c, err := patch(_c.GetExpr())
		if err != nil {
			return nil, err
		}
		_cols[i] = sql.NewCol(__c, name)
	}
	return _cols, nil
}

func hasColumn(cols []sql.SQLObject, name string) bool {
	for _, c := range cols {
		if _c, ok := c.(sql.Aliased); ok && _c.GetAlias() == name {
			return true
		}
	}
	return false
}

type sqlMapInit struct {
	TypeName string
	Keys     []sql.SQLObject
	Values   []sql.SQLObject
}

func (m *sqlMapInit) String(ctx *sql.Ctx, opts ...int) (string, error) {
	str := [][]string{
		make([]string, len(m.Keys)),
		make([]string, len(m.Values)),
	}
	for j, objs := range [][]sql.SQLObject{m.Keys, m.Values} {
		for i, k := range objs {
			var err error
			str[j][i], err = k.String(ctx, opts...)
			if err != nil {
				return "", err
			}
		}
	}

	return fmt.Sprintf("([%s],[%s])::%s",
		strings.Join(str[0], ","),
		strings.Join(str[1], ","),
		m.TypeName), nil
}

type sqlFormat struct {
	format string
	args   []sql.SQLObject
}

func (s *sqlFormat) String(ctx *sql.Ctx, opts ...int) (string, error) {
	args := make([]string, len(s.args))
	for i, a := range s.args {
		var err error
		args[i], err = a.String(ctx, opts...)
		if err != nil {
			return "", err
		}
	}

	format, err := sql.NewStringVal(s.format).String(ctx, opts...)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("format(%s, %s)", format, strings.Join(args, ", ")), nil
}

func getCol(req sql.ISelect, alias string) sql.SQLObject {
	cols := req.GetSelect()
	for _, c := range cols {
		if a, ok := c.(sql.Aliased); ok && a.GetAlias() == alias {
			return a.GetExpr()
		}
	}
	return nil
}

func labelsFromScratch(ctx *shared.PlannerContext, fpCache *sql.With) (sql.ISelect, error) {
	//TODO: offset?
	_from, err := NewTimeSeriesInitPlanner(nil).Process(ctx)
	if err != nil {
		return nil, err
	}
	_from.AndPreWhere(sql.NewIn(sql.NewRawObject("time_series.fingerprint"), sql.NewWithRef(fpCache)))
	return _from, nil
}

func GetTypes(ctx *shared.PlannerContext) *sql.In {
	tp := ctx.Type
	if tp == shared.SAMPLES_TYPE_BOTH {
		tp = shared.SAMPLES_TYPE_LOGS
	}
	return sql.NewIn(sql.NewRawObject("type"), sql.NewIntVal(int64(tp)),
		sql.NewIntVal(shared.SAMPLES_TYPE_BOTH))
}

type UnionAll struct {
	sql.ISelect
	Anothers []sql.ISelect
}

func (u *UnionAll) String(ctx *sql.Ctx, opts ...int) (string, error) {
	selects := make([]string, len(u.Anothers)+1)
	var err error
	selects[0], err = u.ISelect.String(ctx, opts...)
	if err != nil {
		return "", err
	}

	for i, s := range u.Anothers {
		selects[i+1], err = s.String(ctx, opts...)
		if err != nil {
			return "", err
		}
	}

	return strings.Join(selects, " UNION ALL "), nil
}

func FormatFromDate(from time.Time) string {
	return from.UTC().Add(time.Minute * -30).Format("2006-01-02")
}
