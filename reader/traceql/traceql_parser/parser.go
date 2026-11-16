package traceql_parser

import (
	"fmt"
	"strings"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

var parser = participle.MustBuild[TraceQLScript](
	participle.Lexer(TraceQLLexerDefinition), participle.UseLookahead(participle.MaxLookahead))

// misspeltOperators maps operators TraceQL lacks to the TraceQL spelling.
var misspeltOperators = map[string]string{"<<&": "&<<", "<<~": "!<<"}

func Parse(str string) (*TraceQLScript, error) {
	res, err := parser.ParseString("", str+" ")
	if err != nil {
		if misspelt := misspeltOperatorError(str); misspelt != nil {
			return nil, misspelt
		}
		return nil, err
	}
	return res, nil
}

// misspeltOperatorError names the TraceQL spelling of a misspelt operator in str.
func misspeltOperatorError(str string) error {
	lex, err := TraceQLLexerDefinition.Lex("", strings.NewReader(str))
	if err != nil {
		return nil
	}
	tokens, err := lexer.ConsumeAll(lex)
	if err != nil {
		return nil
	}
	for _, tok := range tokens {
		if spelling, ok := misspeltOperators[tok.Value]; ok {
			return fmt.Errorf("%s is not a TraceQL operator, use %s", tok.Value, spelling)
		}
	}
	return nil
}
