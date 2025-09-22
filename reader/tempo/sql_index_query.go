package tempo

import (
	"context"
	"fmt"
	"time"

	"github.com/metrico/qryn/reader/utils/dbversion"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type SQLIndexQuery struct {
	Tags          string
	FromNS        int64
	ToNS          int64
	MinDurationNS int64
	MaxDurationNS int64
	Limit         int64
	Distributed   bool
	Database      string
	Ver           dbversion.VersionInfo
	Ctx           context.Context
}

func (s *SQLIndexQuery) String(ctx *sql.Ctx, options ...int) (string, error) {
	tableName := "`" + s.Database + "`.tempo_traces_attrs_gin"
	if s.Distributed {
		tableName += "_dist"
	}
	var (
		tags *Tags
		err  error
	)
	if s.Tags != "" {
		tags, err = tagsParser.ParseString("", s.Tags)
		if err != nil {
			return "", err
		}
	}
	sqlTagRequests := make([]sql.ISelect, len(tags.Tags))
	for i, tag := range tags.Tags {
		k, err := tag.Name.Parse()
		if err != nil {
			return "", err
		}
		v, err := tag.Val.Parse()
		if err != nil {
			return "", err
		}
		cond := opRegistry[tag.Condition]
		if cond == nil {
			return "", fmt.Errorf("no condition '%s'", tag.Condition)
		}
		sqlTagRequests[i] = sql.NewSelect().
			Select(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id")).
			From(sql.NewRawObject(tableName)).
			AndWhere(
				sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(k)),
				cond(sql.NewStringVal(v)),
				//TODO: move to PRO !!!TURNED OFFF sql.Eq(sql.NewRawObject("oid"), sql.NewStringVal(s.Oid)),
			)
		if s.Limit > 0 && s.Ver.IsVersionSupported("tempo_v2", s.FromNS, s.ToNS) {
			sqlTagRequests[i].Select(
				append(sqlTagRequests[i].GetSelect(), sql.NewRawObject("timestamp_ns"))...)
		}
		if s.FromNS > 0 {
			from := time.Unix(s.FromNS/1e9, s.FromNS%1e9)
			date := fmt.Sprintf("toDate('%s')", from.Format("2006-01-02"))
			sqlTagRequests[i].AndWhere(
				sql.Ge(sql.NewRawObject("date"), sql.NewRawObject(date)),
			)
			if s.Ver.IsVersionSupported("tempo_v2", s.FromNS, s.ToNS) {
				sqlTagRequests[i].AndWhere(
					sql.Ge(sql.NewRawObject("timestamp_ns"), sql.NewIntVal(s.FromNS)))
			}
		}
		if s.ToNS > 0 {
			to := time.Unix(s.ToNS/1e9, s.ToNS%1e9)
			date := fmt.Sprintf("toDate('%s')", to.Format("2006-01-02"))
			sqlTagRequests[i].AndWhere(
				sql.Le(sql.NewRawObject("date"), sql.NewRawObject(date)),
			)
			if s.Ver.IsVersionSupported("tempo_v2", s.FromNS, s.ToNS) {
				sqlTagRequests[i].AndWhere(
					sql.Le(sql.NewRawObject("timestamp_ns"), sql.NewIntVal(s.ToNS)))
			}
		}
		if s.MinDurationNS > 0 && s.Ver.IsVersionSupported("tempo_v2", s.FromNS, s.ToNS) {
			sqlTagRequests[i].AndWhere(
				sql.Ge(sql.NewRawObject("duration"), sql.NewIntVal(s.MinDurationNS)))
		}
		if s.MaxDurationNS > 0 && s.Ver.IsVersionSupported("tempo_v2", s.FromNS, s.ToNS) {
			sqlTagRequests[i].AndWhere(
				sql.Lt(sql.NewRawObject("duration"), sql.NewIntVal(s.MaxDurationNS)))
		}
	}
	request := sql.NewSelect().
		Select(sql.NewRawObject("subsel_0.trace_id"), sql.NewRawObject("subsel_0.span_id"))
	for i, subSel := range sqlTagRequests {
		if i == 0 {
			request.From(sql.NewCol(getSubSelect(subSel), "subsel_0"))
			continue
		}
		alias := fmt.Sprintf("subsel_%d", i)
		request.AddJoin(sql.NewJoin("INNER ANY",
			sql.NewCol(getSubSelect(subSel), alias),
			sql.And(
				sql.Eq(sql.NewRawObject("subsel_0.trace_id"), sql.NewRawObject(alias+".trace_id")),
				sql.Eq(sql.NewRawObject("subsel_0.span_id"), sql.NewRawObject(alias+".span_id")),
			),
		))
	}
	if s.Ver.IsVersionSupported("tempo_v2", s.FromNS, s.ToNS) && s.Limit > 0 {
		request.OrderBy(sql.NewOrderBy(sql.NewRawObject("subsel_0.timestamp_ns"), sql.ORDER_BY_DIRECTION_DESC)).
			Limit(sql.NewRawObject(fmt.Sprintf("%d", s.Limit)))
	}
	return request.String(ctx, options...)
}

func getSubSelect(sel sql.SQLObject) sql.SQLObject {
	return sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
		str, err := sel.String(ctx, options...)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("(%s)", str), nil
	})
}

var opRegistry = map[string]func(val sql.SQLObject) sql.SQLCondition{
	"=": func(val sql.SQLObject) sql.SQLCondition {
		return sql.Eq(sql.NewRawObject("val"), val)
	},
	"!=": func(val sql.SQLObject) sql.SQLCondition {
		return sql.Neq(sql.NewRawObject("val"), val)
	},
	"=~": func(val sql.SQLObject) sql.SQLCondition {
		return sql.Eq(sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
			strVal, err := val.String(ctx, options...)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("match(val, %s)", strVal), nil
		}), sql.NewRawObject("1"))
	},
	"!~": func(val sql.SQLObject) sql.SQLCondition {
		return sql.Neq(sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
			strVal, err := val.String(ctx, options...)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("match(val, %s)", strVal), nil
		}), sql.NewRawObject("1"))
	},
}
