package traceql_parser

import (
	"errors"
	"fmt"
	"strings"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

var parser = participle.MustBuild[TraceQLScript](
	participle.Lexer(TraceQLLexerDefinition), participle.UseLookahead(participle.MaxLookahead),
	participle.Map(unquoteLabelName, "Label_name"))

// misspeltOperators maps operators TraceQL lacks to the TraceQL spelling.
var misspeltOperators = map[string]string{"<<&": "&<<", "<<~": "!<<"}

var (
	labelNameToken = TraceQLLexerDefinition.Symbols()["Label_name"]
	spaceToken     = TraceQLLexerDefinition.Symbols()["space"]
	stringTokens   = map[lexer.TokenType]bool{
		TraceQLLexerDefinition.Symbols()["Quoted_string"]: true,
		TraceQLLexerDefinition.Symbols()["Ticked_string"]: true,
	}
	numberTokens = map[lexer.TokenType]bool{
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

// unquoteLabelName decodes the quoted parts of a name token into the plain name.
func unquoteLabelName(tok lexer.Token) (lexer.Token, error) {
	if !strings.Contains(tok.Value, `"`) {
		return tok, nil
	}
	name, err := unquoteName(tok.Value)
	if err != nil {
		return tok, participle.Errorf(tok.Pos, "%s", err)
	}
	tok.Value = name
	return tok, nil
}

// unquoteName drops the quotes of raw and decodes the escapes \" and \\ inside them.
func unquoteName(raw string) (string, error) {
	var sb strings.Builder
	quoted := false
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		switch {
		case c == '"':
			quoted = !quoted
		case quoted && c == '\\':
			i++
			if i == len(raw) || (raw[i] != '"' && raw[i] != '\\') {
				return "", errors.New("invalid escape sequence")
			}
			sb.WriteByte(raw[i])
		default:
			sb.WriteByte(c)
		}
	}
	if quoted {
		return "", errors.New(`unexpected EOF, expecting "`)
	}
	return sb.String(), nil
}

// tokenError names a misspelt operator, a number directly followed by a name,
// or a string after a colon intrinsic's scope in str.
func tokenError(str string) error {
	lex, err := TraceQLLexerDefinition.Lex("", strings.NewReader(str))
	if err != nil {
		return nil
	}
	tokens, err := lexer.ConsumeAll(lex)
	if err != nil {
		return nil
	}
	var prev lexer.Token
	for i, tok := range tokens {
		if spelling, ok := misspeltOperators[tok.Value]; ok {
			return fmt.Errorf("%s is not a TraceQL operator, use %s", tok.Value, spelling)
		}
		if i > 0 && tok.Type == labelNameToken && numberTokens[tokens[i-1].Type] &&
			tokens[i-1].Pos.Offset+len(tokens[i-1].Value) == tok.Pos.Offset {
			return fmt.Errorf("invalid literal %q", tokens[i-1].Value+tok.Value)
		}
		if stringTokens[tok.Type] && prev.Type == labelNameToken && strings.HasSuffix(prev.Value, ":") {
			return errors.New("syntax error: unexpected STRING")
		}
		if tok.Type != spaceToken {
			prev = tok
		}
	}
	return nil
}
