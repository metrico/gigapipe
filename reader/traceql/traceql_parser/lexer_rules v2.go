package traceql_parser

import (
	"github.com/alecthomas/participle/v2/lexer"
)

var TraceQLLexerRulesV2 = []lexer.SimpleRule{
	{"Ocb", `\{`},
	{"Ccb", `\}`},

	{"Ob", `\(`},
	{"Cb", `\)`},

	{"Comma", `,`},

	{"Ge", `>=`},
	{"Le", `<=`},
	// Structural operators: <<& and <<~ must come before Lt (<) to avoid partial match
	{"Ancestor", `<<&`},
	{"NotAncestor", `<<~`},
	{"Gt", `>`},
	{"Lt", `<`},

	// !>> must come before != to avoid partial match
	{"NotDescendant", `!>>`},
	{"Neq", `!=`},
	{"Re", `=~`},
	{"Nre", `!~`},
	{"Eq", `=`},

	// &>> must come before && to avoid partial match
	{"Descendant", `&>>`},
	{"Sibling", `~`},

	// Label_name supports dotted names and colon intrinsics (span:duration).
	// Keywords like true/false/nil are matched as Label_name and handled in the grammar.
	{"Label_name", `(\.[a-zA-Z_][.:a-zA-Z0-9_-]*|[a-zA-Z_][.:a-zA-Z0-9_-]*)`},
	{"Dot", `\.`},

	{"And", `&&`},
	{"Or", `\|\|`},

	{"Pipe", `\|`},

	{"Quoted_string", `"([^"\\]|\\.)*"`},
	{"Ticked_string", "`([^`\\\\]|\\\\.)*`"},

	{"Minus", "-"},
	// Float must come before Integer to correctly lex 3.14 as a single token
	{"Float", `[0-9]+\.[0-9]+`},
	{"Integer", "[0-9]+"},

	{"space", `\s+`},
}

var TraceQLLexerDefinition = lexer.MustSimple(TraceQLLexerRulesV2)
