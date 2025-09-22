package traceql_parser

import (
	"github.com/alecthomas/participle/v2/lexer"
)

var TraceQLLexerRulesV2 = []lexer.SimpleRule{
	{"Ocb", `\{`},
	{"Ccb", `\}`},

	{"Ob", `\(`},
	{"Cb", `\)`},

	{"Ge", `>=`},
	{"Le", `<=`},
	{"Gt", `>`},
	{"Lt", `<`},

	{"Neq", `!=`},
	{"Re", `=~`},
	{"Nre", `!~`},
	{"Eq", `=`},

	{"Label_name", `(\.[a-zA-Z_][.a-zA-Z0-9_-]*|[a-zA-Z_][.a-zA-Z0-9_-]*)`},
	{"Dot", `\.`},

	{"And", `&&`},
	{"Or", `\|\|`},

	{"Pipe", `\|`},

	{"Quoted_string", `"([^"\\]|\\.)*"`},
	{"Ticked_string", "`([^`\\\\]|\\\\.)*`"},

	{"Minus", "-"},
	{"Integer", "[0-9]+"},

	{"space", `\s+`},
}

var TraceQLLexerDefinition = lexer.MustSimple(TraceQLLexerRulesV2)
