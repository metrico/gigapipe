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
	// Compound operators come before their prefixes (maximal munch). <<& and
	// <<~ lex only to be rejected with their TraceQL spelling.
	{"MisspeltUnionAncestor", `<<&`},
	{"MisspeltNotAncestor", `<<~`},
	{"Descendant", `>>`},
	{"Ancestor", `<<`},
	{"Gt", `>`},
	{"Lt", `<`},

	{"NotDescendant", `!>>`},
	{"NotAncestor", `!<<`},
	{"NotChild", `!>`},
	{"NotParent", `!<`},
	{"Neq", `!=`},
	{"Re", `=~`},
	{"Nre", `!~`},
	{"Not", `!`},
	{"Eq", `=`},

	{"UnionDescendant", `&>>`},
	{"UnionAncestor", `&<<`},
	{"UnionChild", `&>`},
	{"UnionParent", `&<`},
	{"UnionSibling", `&~`},
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
	{"Plus", `\+`},
	{"Mul", `\*`},
	{"Div", `/`},
	{"Mod", `%`},
	{"Pow", `\^`},
	// Float must come before Integer to correctly lex 3.14 as a single token
	{"Float", `[0-9]+\.[0-9]+`},
	{"Integer", "[0-9]+"},

	{"space", `\s+`},
}

var TraceQLLexerDefinition = lexer.MustSimple(TraceQLLexerRulesV2)
