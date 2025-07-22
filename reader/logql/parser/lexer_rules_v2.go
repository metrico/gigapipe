package parser

import (
	"github.com/alecthomas/participle/v2/lexer"
)

var LogQLLexerRulesV2 = []lexer.SimpleRule{
	{Name: "Ocb", Pattern: `\{`},
	{Name: "Ccb", Pattern: `\}`},

	{Name: "Ob", Pattern: `\(`},
	{Name: "Cb", Pattern: `\)`},

	{Name: "Osb", Pattern: `\[`},
	{Name: "Csb", Pattern: `\]`},

	{Name: "Ge", Pattern: `>=`},
	{Name: "Le", Pattern: `<=`},
	{Name: "Gt", Pattern: `>`},
	{Name: "Lt", Pattern: `<`},
	{Name: "Deq", Pattern: `==`},

	{Name: "Comma", Pattern: `,`},

	{Name: "Neq", Pattern: `!=`},
	{Name: "Re", Pattern: `=~`},
	{Name: "Nre", Pattern: `!~`},
	{Name: "Eq", Pattern: `=`},

	{Name: "PipeLineFilter", Pattern: `(\|=|\|~|\|>)`},
	{Name: "Pipe", Pattern: `\|`},
	{Name: "Dot", Pattern: `\.`},

	{Name: "Macros_function", Pattern: `_[a-zA-Z0-9_]+`},
	{Name: "Label_name", Pattern: `[a-zA-Z_][a-zA-Z0-9_]*`},
	{Name: "Quoted_string", Pattern: `"([^"\\]|\\.)*"`},
	{Name: "Ticked_string", Pattern: "`([^`\\\\]|\\\\.)*`"},

	{Name: "Integer", Pattern: "[0-9]+"},

	{Name: "space", Pattern: `\s+`},
}

var LogQLLexerDefinition = lexer.MustSimple(LogQLLexerRulesV2)
