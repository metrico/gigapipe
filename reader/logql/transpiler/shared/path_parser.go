package shared

import (
	"fmt"
	"io"
	"strconv"
	"text/scanner"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
	"github.com/metrico/qryn/reader/logql/parser"
)

func JsonPathParamToTypedArray(param string) ([]string, error) {
	parser, err := participle.Build[jsonPath](participle.Lexer(&jsonDefinitionImpl{}))
	if err != nil {
		return nil, err
	}
	oPath, err := parser.ParseString("", param)
	if err != nil {
		return nil, err
	}
	parts := make([]string, len(oPath.Path))
	for i, part := range oPath.Path {
		parts[i], err = part.ToPathPart()
		if err != nil {
			return nil, err
		}
	}
	return parts, nil
}

func JsonPathParamToArray(param string) ([]string, error) {
	parser, err := participle.Build[jsonPath](participle.Lexer(&jsonDefinitionImpl{}))
	if err != nil {
		return []string{}, err
	}
	oPath, err := parser.ParseString("", param)
	if err != nil {
		return []string{}, err
	}
	parts := make([]string, len(oPath.Path))
	for i, part := range oPath.Path {
		parts[i], err = part.String()
		if err != nil {
			return []string{}, err
		}
	}
	return parts, nil
}

type jsonPath struct {
	Path []jsonPathPart `@@+`
}

type jsonPathPart struct {
	Ident string `Dot? @Ident`
	Field string `| OSQBrack @(QStr|TStr) CSQBrack`
	Idx   string `| OSQBrack @Int CSQBrack`
}

func (j *jsonPathPart) String() (string, error) {
	if j.Ident != "" {
		return j.Ident, nil
	}
	if j.Idx != "" {
		i, err := strconv.Atoi(j.Idx)
		return fmt.Sprintf("%d", i+1), err
	}
	return (&parser.QuotedString{Str: j.Field}).Unquote()
}

func (j *jsonPathPart) ToPathPart() (string, error) {
	if j.Ident != "" {
		return j.Ident, nil
	}
	if j.Field != "" {
		if j.Field[0] == '"' {
			return strconv.Unquote(j.Field)
		}
		return j.Field[1 : len(j.Field)-1], nil
	}
	return j.Idx, nil
}

func (j *jsonPathPart) Value() (any, error) {
	if j.Ident != "" {
		return j.Ident, nil
	}
	if j.Idx != "" {
		i, err := strconv.Atoi(j.Idx)
		return i, err
	}
	return (&parser.QuotedString{Str: j.Field}).Unquote()
}

/* ---------- JSON parser ---------------------*/
var symbols = map[string]lexer.TokenType{
	"Ident":    -2,
	"Dot":      46,
	"OSQBrack": 91,
	"CSQBrack": 93,
	"QStr":     -6,
	"TStr":     -7,
	"Int":      -3,
}

type jsonDefinitionImpl struct{}

func (j *jsonDefinitionImpl) Symbols() map[string]lexer.TokenType {
	return symbols
}

func (j *jsonDefinitionImpl) Lex(filename string, r io.Reader) (lexer.Lexer, error) {
	s := scanner.Scanner{}
	s.Init(r)
	return lexer.LexWithScanner("", &s), nil
}
