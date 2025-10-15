package logql_transpiler

import (
	"github.com/metrico/qryn/v4/reader/logql/logql_parser"
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
)

func Transpile(script string) (shared.RequestProcessorChain, error) {
	oScript, err := logql_parser.Parse(script)
	if err != nil {
		return nil, err
	}
	return Plan(oScript)
}
