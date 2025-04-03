package clickhouse_planner

import (
	"fmt"
	"github.com/metrico/qryn/reader/logql/logql_parser"
	"testing"
)

func TestPlanner(t *testing.T) {
	script := "sum(sum_over_time({test_id=\"${testID}_json\"}| json | unwrap str_id [10s]) by (test_id, str_id)) by (test_id) > 100"
	ast, _ := logql_parser.Parse(script)
	fmt.Println(findFirst[logql_parser.StrSelCmd](ast))

}
