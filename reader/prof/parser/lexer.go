package parser

import (
	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

var LogQLLexerRulesV2 = []lexer.SimpleRule{
	{Name: "Ocb", Pattern: `\{`},
	{Name: "Ccb", Pattern: `\}`},
	{Name: "Comma", Pattern: `,`},

	{Name: "Neq", Pattern: `!=`},
	{Name: "Re", Pattern: `=~`},
	{Name: "Nre", Pattern: `!~`},
	{Name: "Eq", Pattern: `=`},

	{Name: "Dot", Pattern: `\.`},

	{Name: "Label_name", Pattern: `[a-zA-Z_][a-zA-Z0-9_]*`},
	{Name: "Quoted_string", Pattern: `"([^"\\]|\\.)*"`},
	{Name: "Ticked_string", Pattern: "`[^`]*`"},

	{Name: "Integer", Pattern: "[0-9]+"},

	{Name: "space", Pattern: `\s+`},
}

var ProfLexerDefinition = lexer.MustSimple(LogQLLexerRulesV2)
var Parser = participle.MustBuild[Script](
	participle.Lexer(ProfLexerDefinition))
