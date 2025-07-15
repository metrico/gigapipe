package tempo

import (
	"strconv"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

var tagsLexer = lexer.MustStateful(lexer.Rules{
	"Root": {
		{Name: "OQuot", Pattern: `"`, Action: lexer.Push("QString")},
		{Name: "Literal", Pattern: `[^ !=~"]+`, Action: nil},
		{Name: "Cond", Pattern: `(!=|=~|!~|=)`, Action: nil},
		{Name: "space", Pattern: `\s+`, Action: nil},
	},
	"QString": {
		{Name: "Escaped", Pattern: `\\.`, Action: nil},
		{Name: "Char", Pattern: `[^"]`, Action: nil},
		{Name: "CQuot", Pattern: `"`, Action: lexer.Pop()},
	},
})

type QuotedString struct {
	Str string
}

type LiteralOrQString struct {
	Literal string `@Literal`
	QString string `| (@OQuot(@Escaped|@Char)*@CQuot)`
}

func (l LiteralOrQString) Parse() (string, error) {
	if l.Literal != "" {
		return l.Literal, nil
	}
	return strconv.Unquote(l.QString)
}

type Tag struct {
	Name      LiteralOrQString `@@`
	Condition string           `@Cond`
	Val       LiteralOrQString `@@`
}

type Tags struct {
	Tags []Tag `@@*`
}

var tagsParser = participle.MustBuild[Tags](
	participle.Lexer(tagsLexer),
	participle.Elide("space"),
)
