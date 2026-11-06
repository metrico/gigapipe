package traceql_parser

import (
	"strings"
	"testing"

	"github.com/alecthomas/participle/v2/lexer"
	"github.com/stretchr/testify/require"
)

// lexTokens returns "Rule:value" for each token of q, without whitespace.
func lexTokens(t *testing.T, q string) []string {
	t.Helper()
	names := map[lexer.TokenType]string{}
	for name, tokenType := range TraceQLLexerDefinition.Symbols() {
		names[tokenType] = name
	}
	lex, err := TraceQLLexerDefinition.Lex("", strings.NewReader(q))
	require.NoError(t, err)
	tokens, err := lexer.ConsumeAll(lex)
	require.NoError(t, err)
	var got []string
	for _, tok := range tokens {
		if tok.EOF() || names[tok.Type] == "space" {
			continue
		}
		got = append(got, names[tok.Type]+":"+tok.Value)
	}
	return got
}

func TestLexNumbersAndAttributes(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  []string
	}{
		{`.95`, []string{"Float:.95"}},
		{`.5`, []string{"Float:.5"}},
		{`5.`, []string{"Float:5."}},
		{`1.5`, []string{"Float:1.5"}},
		{`1e3`, []string{"Float:1e3"}},
		{`1E+3`, []string{"Float:1E+3"}},
		{`1.5e-3`, []string{"Float:1.5e-3"}},
		{`.5e2`, []string{"Float:.5e2"}},
		{`5`, []string{"Integer:5"}},
		{`-.5`, []string{"Minus:-", "Float:.5"}},
		{`.5s`, []string{"Duration:.5s"}},
		{`5.s`, []string{"Duration:5.s"}},
		{`1h30m`, []string{"Duration:1h30m"}},
		{`1h15m30.918273645s`, []string{"Duration:1h15m30.918273645s"}},
		{`1µs`, []string{"Duration:1µs"}},
		{`1h30`, []string{"Duration:1h30"}},
		{`1.5d`, []string{"Duration:1.5d"}},
		{`1h.5`, []string{"Duration:1h.5"}},
		{`1hx`, []string{"Duration:1h", "Label_name:x"}},
		{`1e3s`, []string{"Float:1e3", "Label_name:s"}},
		{`1x`, []string{"Integer:1", "Label_name:x"}},
		{`5 s`, []string{"Integer:5", "Label_name:s"}},
		{`span.5s`, []string{"Label_name:span.5s"}},
		{`.5xx`, []string{"Float:.5", "Label_name:xx"}},
		{`.5.5`, []string{"Float:.5", "Float:.5"}},
		{`1.2.3`, []string{"Float:1.2", "Float:.3"}},
		{`1e`, []string{"Integer:1", "Label_name:e"}},
		{`5 . 5`, []string{"Integer:5", "Dot:.", "Integer:5"}},
		{`.foo.95`, []string{"Label_name:.foo.95"}},
		{`span.5xx`, []string{"Label_name:span.5xx"}},
		{`.foo-.5`, []string{"Label_name:.foo-.5"}},
		{`.foo - .5`, []string{"Label_name:.foo", "Minus:-", "Float:.5"}},
		{`.foo * .5`, []string{"Label_name:.foo", "Mul:*", "Float:.5"}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			require.Equal(t, tc.want, lexTokens(t, tc.query))
		})
	}
}
