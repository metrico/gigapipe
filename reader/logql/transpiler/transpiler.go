package transpiler

import (
	"github.com/metrico/qryn/reader/logql/transpiler/shared"
	"github.com/metrico/qryn/reader/logql/parser"
)

func Transpile(script string) (shared.RequestProcessorChain, error) {
	oScript, err := parser.Parse(script)
	if err != nil {
		return nil, err
	}
	return Plan(oScript)
}
