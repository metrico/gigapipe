package clickhouse_planner

import (
	"fmt"
	"testing"

	parser "github.com/metrico/qryn/reader/logql/parser"
)

func TestPlanner(t *testing.T) {
	script := "sum(sum_over_time({test_id=\"${testID}_json\"}| json | unwrap str_id [10s]) by (test_id, str_id)) by (test_id) > 100"
	ast, _ := parser.Parse(script)
	fmt.Println(findFirst[parser.StrSelCmd](ast))

}
