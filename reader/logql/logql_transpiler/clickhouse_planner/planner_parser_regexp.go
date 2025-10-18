package clickhouse_planner

import (
	"fmt"
	"strings"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

func (p *ParserPlanner) regexp(ctx *shared.PlannerContext) (sql.ISelect, error) {
	req, err := p.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	ast, err := p.parseRe(p.Vals[0])
	if err != nil {
		return nil, err
	}

	names := ast.collectGroupNames(nil)

	sel, err := patchCol(req.GetSelect(), "labels", func(object sql.SQLObject) (sql.SQLObject, error) {
		return &sqlMapUpdate{
			m1: object,
			m2: &regexMap{
				col:    sql.NewRawObject("string"),
				labels: names,
				re:     ast.String(),
			},
		}, nil
	})
	if err != nil {
		return nil, err
	}

	return req.Select(sel...), nil
}

func (p *ParserPlanner) parseRe(re string) (*regexAST, error) {
	parser, err := participle.Build[regexAST](participle.Lexer(regexParserDesc))
	if err != nil {
		return nil, err
	}
	res, err := parser.ParseString("", re)
	return res, err
}

var regexParserDesc = lexer.MustSimple([]lexer.SimpleRule{
	{Name: "OBrackQ", Pattern: "\\(\\?P<"},
	{Name: "OBrack", Pattern: "\\("},
	{Name: "CBrack", Pattern: "\\)"},
	{Name: "CCBrack", Pattern: ">"},
	{Name: "Ident", Pattern: "[a-zA-Z_][0-9a-zA-Z_]*"},
	{Name: "Char", Pattern: `\\.|.`},
})

type regexAST struct {
	RegexPart []regexPart `@@+`
}

func (r *regexAST) String() string {
	res := make([]string, len(r.RegexPart))
	for i, r := range r.RegexPart {
		res[i] = r.String()
	}
	return strings.Join(res, "")
}
func (r *regexAST) collectGroupNames(init []string) []string {
	for _, p := range r.RegexPart {
		init = p.collectGroupNames(init)
	}
	return init
}

type regexPart struct {
	SimplePart     string     `@(Char|CCBrack|Ident)+`
	NamedBrackPart *brackPart `| OBrackQ @@ CBrack`
	BrackPart      *regexAST  `| OBrack @@ CBrack`
}

func (r *regexPart) String() string {
	if r.SimplePart != "" {
		return r.SimplePart
	}
	if r.NamedBrackPart != nil {
		return "(" + r.NamedBrackPart.String() + ")"
	}
	return "(" + r.BrackPart.String() + ")"
}
func (r *regexPart) collectGroupNames(init []string) []string {
	if r.NamedBrackPart != nil {
		return r.NamedBrackPart.collectGroupNames(init)
	}
	if r.BrackPart != nil {
		init = append(init, "")
		return r.BrackPart.collectGroupNames(init)
	}
	return init
}

type brackPart struct {
	Name string    `@Ident CCBrack`
	Tail *regexAST `@@?`
}

func (b *brackPart) String() string {
	return b.Tail.String()
}
func (b *brackPart) collectGroupNames(init []string) []string {
	init = append(init, b.Name)
	init = b.Tail.collectGroupNames(init)
	return init
}

type regexMap struct {
	col    sql.SQLObject
	labels []string
	re     string
}

func (r *regexMap) String(ctx *sql.Ctx, opts ...int) (string, error) {
	strCol, err := r.col.String(ctx, opts...)
	if err != nil {
		return "", err
	}

	strLabels := make([]string, len(r.labels))
	for i, l := range r.labels {
		var err error
		strLabels[i], err = (sql.NewStringVal(l)).String(ctx, opts...)
		if err != nil {
			return "", err
		}
	}

	strRe, err := (sql.NewStringVal(r.re)).String(ctx, opts...)
	if err != nil {
		return "", err
	}

	id := ctx.Id()

	return fmt.Sprintf("mapFromArrays("+
		"arrayFilter("+
		" (x,y) -> x != '' AND y != '',"+
		"  [%[1]s] as re_lbls_%[2]d,"+
		"  arrayMap(x -> x[length(x)], extractAllGroupsHorizontal(%[4]s, %[3]s)) as re_vals_%[2]d),"+
		"arrayFilter((x,y) -> x != '' AND y != '', re_vals_%[2]d, re_lbls_%[2]d))",
		strings.Join(strLabels, ","),
		id,
		strRe,
		strCol), nil
}
