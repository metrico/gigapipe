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
//
// Duration and bytes expressions return NULL (not 0) when the input is empty
// or garbage, so the downstream WHERE value IS NOT NULL filter drops them.
func unwrapConvSQL(strLabel string, convFunc string) string {
	switch convFunc {
	case "duration", "duration_seconds":
		// Parse multi-part Go duration strings. The regex lists multi-char
		// units (ms, us, µs, ns) before single-char ones (h, m, s) so RE2's
		// leftmost-first alternation disambiguates correctly.
		// Returns NULL when no unit groups match (empty, garbage, "1d").
		// Handles leading minus sign by checking startsWith and negating.
		re := "'([0-9]*\\\\.?[0-9]+)(ms|us|µs|ns|h|m|s)'"
		return fmt.Sprintf(
			"if(length(extractAllGroupsHorizontal(%[1]s, %[2]s)[1]) > 0, "+
				"if(startsWith(%[1]s, '-'), -1, 1) * "+
				"arraySum((v, u) -> toFloat64OrZero(v) * multiIf("+
				"u = 'h', 3600, u = 'm', 60, u = 's', 1, "+
				"u = 'ms', 0.001, u = 'us', 0.000001, u = 'µs', 0.000001, "+
				"u = 'ns', 0.000000001, 0), "+
				"extractAllGroupsHorizontal(%[1]s, %[2]s)[1], "+
				"extractAllGroupsHorizontal(%[1]s, %[2]s)[2]), "+
				"NULL)",
			strLabel, re)
	case "bytes":
		// Strip commas (go-humanize accepts "1,024 KB"), extract the leading
		// number and the unit suffix separately. Returns NULL when no number
		// is found or the unit is unrecognised.
		stripped := fmt.Sprintf("replaceAll(lower(%s), ',', '')", strLabel)
		num := fmt.Sprintf("extract(%s, '([0-9]*\\\\.?[0-9]+)')", stripped)
		unit := fmt.Sprintf("trimBoth(replaceRegexpOne(%s, '^[ 0-9.,]+', ''))", stripped)
		return fmt.Sprintf(
			"if(%[1]s = '', NULL, toFloat64OrZero(%[1]s) * multiIf("+
				"%[2]s = 'eib', 1152921504606846976, "+
				"%[2]s = 'pib', 1125899906842624, "+
				"%[2]s = 'tib', 1099511627776, "+
				"%[2]s = 'gib', 1073741824, "+
				"%[2]s = 'mib', 1048576, "+
				"%[2]s = 'kib', 1024, "+
				"%[2]s = 'ei', 1152921504606846976, "+
				"%[2]s = 'pi', 1125899906842624, "+
				"%[2]s = 'ti', 1099511627776, "+
				"%[2]s = 'gi', 1073741824, "+
				"%[2]s = 'mi', 1048576, "+
				"%[2]s = 'ki', 1024, "+
				"%[2]s = 'eb', 1000000000000000000, "+
				"%[2]s = 'pb', 1000000000000000, "+
				"%[2]s = 'tb', 1000000000000, "+
				"%[2]s = 'gb', 1000000000, "+
				"%[2]s = 'mb', 1000000, "+
				"%[2]s = 'kb', 1000, "+
				"%[2]s = 'e', 1000000000000000000, "+
				"%[2]s = 'p', 1000000000000000, "+
				"%[2]s = 't', 1000000000000, "+
				"%[2]s = 'g', 1000000000, "+
				"%[2]s = 'm', 1000000, "+
				"%[2]s = 'k', 1000, "+
				"%[2]s = 'b', 1, "+
				"%[2]s = '', 1, "+
				"NULL))",
			num, unit)
	default:
		return fmt.Sprintf("toFloat64OrNull(%s)", strLabel)
	}
}
