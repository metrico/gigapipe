package clickhouse_transpiler

import (
	"fmt"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	traceql_parser "github.com/metrico/qryn/reader/traceql/parser"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
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

func checkLabelSupport(label *traceql_parser.LabelName) error {
	if label.Path()[0] == "span" {
		return nil
	}
	if label.Path()[0] == "resource" {
		return nil
	}
	if label.Parts[0][0] == '.' {
		return nil
	}
	if len(label.Path()) == 1 {
		switch label.Path()[0] {
		case "name", "duration", "nestedSetParent":
			return nil
		}
	}
	return labelNotSupportedError
}
