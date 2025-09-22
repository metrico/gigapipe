package prof_parser

import (
	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

var LogQLLexerRulesV2 = []lexer.SimpleRule{
	{"Ocb", `\{`},
	{"Ccb", `\}`},
	{"Comma", `,`},

	{"Neq", `!=`},
	{"Re", `=~`},
	{"Nre", `!~`},
	{"Eq", `=`},

	{"Dot", `\.`},

	{"Label_name", `[a-zA-Z_][a-zA-Z0-9_]*`},
	{"Quoted_string", `"([^"\\]|\\.)*"`},
	{"Ticked_string", "`[^`]*`"},

	{"Integer", "[0-9]+"},

	{"space", `\s+`},
}

var ProfLexerDefinition = lexer.MustSimple(LogQLLexerRulesV2)
var Parser = participle.MustBuild[Script](
	participle.Lexer(ProfLexerDefinition))
