package prof_parser

import "regexp"

var parseReg = regexp.MustCompile(
	"^\\{\\s*([a-zA-Z_][0-9a-zA-Z_]*)\\s*(=~|!~|=|!=)\\s*(`[^`]+`|\"([^\"]|\\.)*\")(\\s*,\\s*([a-zA-Z_][0-9a-zA-Z_]*)\\s*(=~|!~|=|!=)\\s*(`[^`]+`|\"([^\"]|\\.)*\"))*}$",
)

func Parse(query string) (*Script, error) {
	return Parser.ParseString("", query)
}
