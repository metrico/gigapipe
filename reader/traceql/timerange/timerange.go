// Package timerange holds the query time-range predicates for TraceQL search,
// autocomplete and metrics.
package timerange

import (
	"time"

	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// SpanStart returns the lower and upper bounds restricting col, a span start
// time in ns, to (fromNs, toNs].
func SpanStart(col string, fromNs, toNs int64) (lower, upper sql.SQLCondition) {
	c := sql.NewRawObject(col)
	return sql.Gt(c, sql.NewIntVal(fromNs)), sql.Le(c, sql.NewIntVal(toNs))
}

// SpanDate returns bounds on col, a span's stored date in any time zone: a day
// either side of the UTC dates of from and to, a superset of (from, to].
func SpanDate(col string, from, to time.Time) (lower, upper sql.SQLCondition) {
	c := sql.NewRawObject(col)
	return sql.Ge(c, sql.NewStringVal(from.UTC().AddDate(0, 0, -1).Format(time.DateOnly))),
		sql.Le(c, sql.NewStringVal(to.UTC().AddDate(0, 0, 1).Format(time.DateOnly)))
}
