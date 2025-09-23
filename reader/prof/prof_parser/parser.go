package prof_parser

func Parse(query string) (*Script, error) {
	return Parser.ParseString("", query)
}
