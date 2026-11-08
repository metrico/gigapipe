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

// TreeReadSkew is how long after the end of a range the ancestry tree read
// reaches: a parent may start that much after its child.
const TreeReadSkew = time.Hour

// TreeRead returns the bound restricting col, a span start time in ns, for a
// tree read of a range ending at toNs. It has no lower bound: ancestors of a
// span in range may start any time before it.
func TreeRead(col string, toNs int64) sql.SQLCondition {
	return sql.Le(sql.NewRawObject(col), sql.NewIntVal(toNs+int64(TreeReadSkew)))
}
