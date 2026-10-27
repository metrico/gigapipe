package clickhouse_planner

import (
	"fmt"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

type UnwrapPlanner struct {
	Main               shared.SQLRequestPlanner
	Label              string
	ConvFunc           string
	UseTimeSeriesTable bool
	labelsCache        **sql.With
	fpCache            **sql.With
}

func (u *UnwrapPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	mainReq, err := u.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	if u.UseTimeSeriesTable {
		return u.processTimeSeries(ctx, mainReq)
	} else {
		return u.processSimple(ctx, mainReq)
	}
}

func (u *UnwrapPlanner) processSimple(_ *shared.PlannerContext, main sql.ISelect) (sql.ISelect, error) {
	sel := main.GetSelect()
	labels := getCol(main, "labels")
	strCol := getCol(main, "string")
	if labels == nil {
		return nil, fmt.Errorf("labels col not inited")
	}
	label := u.Label

	sel, err := patchCol(sel, "value", func(object sql.SQLObject) (sql.SQLObject, error) {
		return sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
			var strLabel string
			if u.Label != "_entry" {
				strLabels, err := labels.String(ctx, options...)
				if err != nil {
					return "", err
				}
				val, err := sql.NewStringVal(label).String(ctx, options...)
				if err != nil {
					return "", err
				}
				strLabel = fmt.Sprintf("%s[%s]", strLabels, val)
			} else {
				var err error
				strLabel, err = strCol.String(ctx, options...)
				if err != nil {
					return "", err
				}
			}
			return unwrapConvSQL(strLabel, u.ConvFunc), nil
		}), nil
	})
	if err != nil {
		return nil, err
	}

	return main.Select(sel...).AndWhere(sql.NotNull(sql.NewRawObject("value"))), nil
}

func (u *UnwrapPlanner) processTimeSeries(ctx *shared.PlannerContext, main sql.ISelect) (sql.ISelect, error) {
	var from sql.SQLObject
	if *u.labelsCache != nil {
		from = sql.NewWithRef(*u.labelsCache)
	} else {
		var err error
		from, err = labelsFromScratch(ctx, *u.fpCache, nil)
		if err != nil {
			return nil, err
		}
	}
	// TODO:  remove this
	subSelect := sql.NewSelect().Select(
		sql.NewRawObject("fingerprint"),
		sql.NewRawObject("labels"),
	).From(from)

	subSelect, err := u.processSimple(ctx, main)
	if err != nil {
		return nil, err
	}

	joinType := "ANY LEFT "
	if ctx.IsCluster {
		joinType = "GLOBAL ANY LEFT "
	}

	main.Select(append(main.GetSelect(),
		sql.NewSimpleCol("sub_value.value", "value"),
		sql.NewSimpleCol("sub_value.labels", "labels"))...).
		Join(sql.NewJoin(joinType, sql.NewCol(subSelect, "sub_value"),
			sql.Eq(sql.NewRawObject("fingerprint"), sql.NewRawObject("sub_value.fingerprint"))))
	return main, nil
}

// unwrapConvSQL returns a ClickHouse SQL expression that converts a string
// label value to a Float64. When convFunc is empty, the label is parsed as a
// plain number. The conversion functions match Loki's unwrap semantics:
//   - duration / duration_seconds: Go duration string ("5m30s") → seconds
//   - bytes: byte-size string ("1.5MB") → bytes
func unwrapConvSQL(strLabel string, convFunc string) string {
	switch convFunc {
	case "duration", "duration_seconds":
		// Parse multi-part Go duration strings. The regex lists multi-char
		// units (ms, us, ns) before single-char ones (h, m, s) so RE2's
		// leftmost-first alternation disambiguates correctly.
		return fmt.Sprintf(
			"arraySum((v, u) -> toFloat64OrZero(v) * multiIf("+
				"u = 'h', 3600, u = 'm', 60, u = 's', 1, "+
				"u = 'ms', 0.001, u = 'us', 0.000001, "+
				"u = 'ns', 0.000000001, 0), "+
				"extractAllGroupsHorizontal(%s, '([0-9]*\\\\.?[0-9]+)(ms|us|ns|h|m|s)')[1], "+
				"extractAllGroupsHorizontal(%s, '([0-9]*\\\\.?[0-9]+)(ms|us|ns|h|m|s)')[2])",
			strLabel, strLabel)
	case "bytes":
		// Extract the leading number with a single regex, then use LIKE
		// checks for the unit suffix. Longer suffixes (kib, mib, …) are
		// checked before shorter ones (kb, mb, …) to avoid false matches.
		return fmt.Sprintf(
			"toFloat64OrZero(extract(lower(%[1]s), '([0-9]*\\\\.?[0-9]+)')) * multiIf("+
				"lower(%[1]s) LIKE '%%pib%%', 1125899906842624, "+
				"lower(%[1]s) LIKE '%%tib%%', 1099511627776, "+
				"lower(%[1]s) LIKE '%%gib%%', 1073741824, "+
				"lower(%[1]s) LIKE '%%mib%%', 1048576, "+
				"lower(%[1]s) LIKE '%%kib%%', 1024, "+
				"lower(%[1]s) LIKE '%%pb%%', 1000000000000000, "+
				"lower(%[1]s) LIKE '%%tb%%', 1000000000000, "+
				"lower(%[1]s) LIKE '%%gb%%', 1000000000, "+
				"lower(%[1]s) LIKE '%%mb%%', 1000000, "+
				"lower(%[1]s) LIKE '%%kb%%', 1000, "+
				"1)", strLabel)
	default:
		return fmt.Sprintf("toFloat64OrNull(%s)", strLabel)
	}
}
