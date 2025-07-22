package parser

import (
	"github.com/alecthomas/participle/v2/lexer"
)

var TraceQLLexerRulesV2 = []lexer.SimpleRule{
	{Name: "Ocb", Pattern: `\{`},
	{Name: "Ccb", Pattern: `\}`},

	{Name: "Ob", Pattern: `\(`},
	{Name: "Cb", Pattern: `\)`},

	{Name: "Ge", Pattern: `>=`},
	{Name: "Le", Pattern: `<=`},
	{Name: "Gt", Pattern: `>`},
	{Name: "Lt", Pattern: `<`},

	{Name: "Neq", Pattern: `!=`},
	{Name: "Re", Pattern: `=~`},
	{Name: "Nre", Pattern: `!~`},
	{Name: "Eq", Pattern: `=`},

	{Name: "Label_name", Pattern: `(\.[a-zA-Z_][.a-zA-Z0-9_-]*|[a-zA-Z_][.a-zA-Z0-9_-]*)`},
	{Name: "Dot", Pattern: `\.`},

	{Name: "And", Pattern: `&&`},
	{Name: "Or", Pattern: `\|\|`},

	{Name: "Pipe", Pattern: `\|`},

	{Name: "Quoted_string", Pattern: `"([^"\\]|\\.)*"`},
	{Name: "Ticked_string", Pattern: "`([^`\\\\]|\\\\.)*`"},

	{Name: "Minus", Pattern: "-"},
	{Name: "Integer", Pattern: "[0-9]+"},

	{Name: "space", Pattern: `\s+`},
}

var TraceQLLexerDefinition = lexer.MustSimple(TraceQLLexerRulesV2)
