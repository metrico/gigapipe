package clickhouse_planner

import (
	"fmt"
	"text/template"
	"text/template/parse"

	"github.com/metrico/qryn/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type LineFormatPlanner struct {
	Main     shared.SQLRequestPlanner
	Template string

	formatStr string
	args      []sql.SQLObject
}

func (l *LineFormatPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := l.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	err = l.ProcessTpl(ctx)
	if err != nil {
		return nil, err
	}

	sel, err := patchCol(main.GetSelect(), "string", func(object sql.SQLObject) (sql.SQLObject, error) {
		return &sqlFormat{
			format: l.formatStr,
			args:   l.args,
		}, nil
	})
	if err != nil {
		return nil, err
	}
	return main.Select(sel...), nil
}

func (l *LineFormatPlanner) ProcessTpl(ctx *shared.PlannerContext) error {
	tpl, err := template.New(fmt.Sprintf("tpl%d", ctx.Id())).Parse(l.Template)
	if err != nil {
		return err
	}

	return l.visitNodes(tpl.Root, l.node)
}

func (l *LineFormatPlanner) IsSupported() bool {
	tpl, err := template.New("tpl1").Parse(l.Template)
	if err != nil {
		return false
	}
	err = l.visitNodes(tpl.Root, func(n parse.Node) error {
		switch n.Type() {
		case parse.NodeList:
		case parse.NodeAction:
			if len(n.(*parse.ActionNode).Pipe.Cmds) > 1 || len(n.(*parse.ActionNode).Pipe.Cmds[0].Args) > 1 {
				return fmt.Errorf("not supported")
			}
		case parse.NodeField:
		case parse.NodeText:
		default:
			return fmt.Errorf("not supported")
		}
		return nil
	})
	return err == nil
}

func (l *LineFormatPlanner) visitNodes(n parse.Node, fn func(n parse.Node) error) error {
	err := fn(n)
	if err != nil {
		return err
	}
	switch n.Type() {
	case parse.NodeList:
		for _, _n := range n.(*parse.ListNode).Nodes {
			err := l.visitNodes(_n, fn)
			if err != nil {
				return err
			}
		}
	case parse.NodeAction:
		for _, cmd := range n.(*parse.ActionNode).Pipe.Cmds {
			for _, arg := range cmd.Args {
				err := l.visitNodes(arg, fn)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (l *LineFormatPlanner) node(n parse.Node) error {
	switch n.Type() {
	case parse.NodeText:
		l.textNode(n)
	case parse.NodeField:
		l.fieldNode(n)
	}
	return nil
}

func (l *LineFormatPlanner) textNode(n parse.Node) {
	l.formatStr += string(n.(*parse.TextNode).Text)
}

func (l *LineFormatPlanner) fieldNode(n parse.Node) {
	l.formatStr += fmt.Sprintf("{%d}", len(l.args))
	l.args = append(l.args, sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
		lbl, err := sql.NewStringVal(n.(*parse.FieldNode).Ident[0]).String(ctx, options...)
		return fmt.Sprintf("labels[%s]", lbl), err
	}))
}
