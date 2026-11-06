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

var (
	labelNameToken = TraceQLLexerDefinition.Symbols()["Label_name"]
	numberTokens   = map[lexer.TokenType]bool{
		TraceQLLexerDefinition.Symbols()["Float"]:    true,
		TraceQLLexerDefinition.Symbols()["Integer"]:  true,
		TraceQLLexerDefinition.Symbols()["Duration"]: true,
	}
)

func Parse(str string) (*TraceQLScript, error) {
	res, err := parser.ParseString("", str+" ")
	if err != nil {
		if named := tokenError(str); named != nil {
			return nil, named
		}
		return nil, err
	}
	return res, nil
}

// tokenError names a misspelt operator or a number directly followed by a name
// in str.
func tokenError(str string) error {
	lex, err := TraceQLLexerDefinition.Lex("", strings.NewReader(str))
	if err != nil {
		return nil
	}
	tokens, err := lexer.ConsumeAll(lex)
	if err != nil {
		return nil
	}
	for i, tok := range tokens {
		if spelling, ok := misspeltOperators[tok.Value]; ok {
			return fmt.Errorf("%s is not a TraceQL operator, use %s", tok.Value, spelling)
		}
		if i > 0 && tok.Type == labelNameToken && numberTokens[tokens[i-1].Type] &&
			tokens[i-1].Pos.Offset+len(tokens[i-1].Value) == tok.Pos.Offset {
			return fmt.Errorf("invalid literal %q", tokens[i-1].Value+tok.Value)
		}
	}
	return nil
}
