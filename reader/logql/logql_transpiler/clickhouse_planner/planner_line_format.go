package clickhouse_planner

import (
	"fmt"
	"text/template"
	"text/template/parse"
	"time"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
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
	tpl, err := template.New(fmt.Sprintf("tpl%d", ctx.Id())).Funcs(lineFormatParseFuncs()).Parse(l.Template)
	if err != nil {
		return err
	}

	return l.visitNodes(tpl.Root, l.node)
}

func (l *LineFormatPlanner) IsSupported() bool {
	tpl, err := template.New("tpl1").Funcs(lineFormatParseFuncs()).Parse(l.Template)
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
	case parse.NodeAction:
		l.actionNode(n)
	}
	return nil
}

func (l *LineFormatPlanner) textNode(n parse.Node) {
	l.formatStr += string(n.(*parse.TextNode).Text)
}

func (l *LineFormatPlanner) fieldNode(n parse.Node) {
	switch n.(*parse.FieldNode).Ident[0] {
	case "__line__", "_entry":
		l.appendLineArg()
	case "__timestamp__":
		l.appendTimestampArg()
	default:
		l.appendLabelArg(n.(*parse.FieldNode).Ident[0])
	}
}

func (l *LineFormatPlanner) actionNode(n parse.Node) {
	action := n.(*parse.ActionNode)
	if len(action.Pipe.Cmds) != 1 {
		return
	}
	cmd := action.Pipe.Cmds[0]
	if len(cmd.Args) != 1 {
		return
	}
	if ident, ok := cmd.Args[0].(*parse.IdentifierNode); ok {
		switch ident.Ident {
		case "__line__", "_entry":
			l.appendLineArg()
		case "__timestamp__":
			l.appendTimestampArg()
		}
	}
}

func (l *LineFormatPlanner) appendLabelArg(label string) {
	l.formatStr += fmt.Sprintf("{%d}", len(l.args))
	l.args = append(l.args, sql.NewCustomCol(func(ctx *sql.Ctx, options ...int) (string, error) {
		lbl, err := sql.NewStringVal(label).String(ctx, options...)
		return fmt.Sprintf("labels[%s]", lbl), err
	}))
}

func (l *LineFormatPlanner) appendLineArg() {
	l.formatStr += fmt.Sprintf("{%d}", len(l.args))
	l.args = append(l.args, sql.NewRawObject("string"))
}

func (l *LineFormatPlanner) appendTimestampArg() {
	l.formatStr += fmt.Sprintf("{%d}", len(l.args))
	l.args = append(l.args, sql.NewRawObject("fromUnixTimestamp64Nano(timestamp_ns)"))
}

func lineFormatParseFuncs() template.FuncMap {
	funcs := shared.BaseTemplateFuncs()
	funcs["__line__"] = func() string { return "" }
	funcs["__timestamp__"] = func() time.Time { return time.Time{} }
	return funcs
}
