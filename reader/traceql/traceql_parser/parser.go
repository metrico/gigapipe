package traceql_parser

import (
	"github.com/alecthomas/participle/v2"
)

func Parse(str string) (*TraceQLScript, error) {
	res := &TraceQLScript{}
	parser, err := participle.Build[TraceQLScript](participle.Lexer(TraceQLLexerDefinition), participle.UseLookahead(5))
	if err != nil {
		return nil, err
	}
	res, err = parser.ParseString("", str+" ")
	return res, err
}
