package logql_parser

import (
	"github.com/alecthomas/participle/v2/lexer"
)

var LogQLLexerRulesV2 = []lexer.SimpleRule{
	{"Ocb", `\{`},
	{"Ccb", `\}`},

	{"Ob", `\(`},
	{"Cb", `\)`},

	{"Osb", `\[`},
	{"Csb", `\]`},

	{"Ge", `>=`},
	{"Le", `<=`},
	{"Gt", `>`},
	{"Lt", `<`},
	{"Deq", `==`},

	{"Comma", `,`},

	{"Neq", `!=`},
	{"Re", `=~`},
	{"Nre", `!~`},
	{"Eq", `=`},

	{"PipeLineFilter", `(\|=|\|~|\|>)`},
	{"Pipe", `\|`},
	{"Dot", `\.`},

	{"Macros_function", `_[a-zA-Z0-9_]+`},
	{"Label_name", `[a-zA-Z_][a-zA-Z0-9_]*`},
	{"Quoted_string", `"([^"\\]|\\.)*"`},
	{"Ticked_string", "`([^`\\\\]|\\\\.)*`"},

	{"Integer", "[0-9]+"},

	{"space", `\s+`},
}

var LogQLLexerDefinition = lexer.MustSimple(LogQLLexerRulesV2)
