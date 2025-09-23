package planner

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler/clickhouse_planner"
)

type StreamSelectPlanner struct {
	clickhouse_planner.StreamSelectPlanner
}
