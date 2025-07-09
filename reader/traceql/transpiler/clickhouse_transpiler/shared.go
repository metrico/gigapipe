package clickhouse_transpiler

import (
	"fmt"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	traceql_parser "github.com/metrico/qryn/reader/traceql/parser"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
	"k8s.io/utils/strings/slices"
)

func getComparisonFn(op string) (func(left sql.SQLObject, right sql.SQLObject) *sql.LogicalOp, error) {
	switch op {
	case "=":
		return sql.Eq, nil
	case ">":
		return sql.Gt, nil
	case "<":
		return sql.Lt, nil
	case ">=":
		return sql.Ge, nil
	case "<=":
		return sql.Le, nil
	case "!=":
		return sql.Neq, nil
	}
	return nil, &shared.NotSupportedError{Msg: "not supported operator: " + op}
}

var labelNotSupportedError = fmt.Errorf("attribute not supported")

func isScoped(label *traceql_parser.LabelName) bool {
	return len(label.Path()) > 1 && slices.Contains([]string{"span", "resource"}, label.Path()[0])
}

func isUnscoped(label *traceql_parser.LabelName) bool {
	return len(label.Parts) >= 1 && label.Parts[0][0] == '.'
}

func isSupportedIntrinsic(label *traceql_parser.LabelName) bool {
	return len(label.Parts) == 1 && slices.Contains([]string{"name", "duration", "status"}, label.Parts[0])
}

func checkLabelSupport(label *traceql_parser.LabelName) error {
	if isScoped(label) || isUnscoped(label) || isSupportedIntrinsic(label) {
		return nil
	}
	return labelNotSupportedError
}
