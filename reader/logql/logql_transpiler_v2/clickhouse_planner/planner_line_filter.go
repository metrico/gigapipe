package clickhouse_planner

import (
	"fmt"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
	"regexp/syntax"
	"strings"
)

type LineFilterPlanner struct {
	Op   string
	Val  string
	Main shared.SQLRequestPlanner
}

func (l *LineFilterPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	req, err := l.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	var clause sql.SQLCondition
	switch l.Op {
	case "|=":
		clause, err = l.doLike("like")
		break
	case "!=":
		clause, err = l.doLike("notLike")
		break
	case "|~":
		likeStr, isInsensitive, isLike := l.re2Like()
		if isLike {
			l.Val = likeStr
			like := "like"
			if isInsensitive {
				like = "ilike"
			}
			clause, err = l.doLike(like)
		} else {
			clause = sql.Eq(&sqlMatch{
				col:     sql.NewRawObject("string"),
				pattern: l.Val,
			}, sql.NewIntVal(1))
		}
		break
	case "!~":
		likeStr, isInsensitive, isLike := l.re2Like()
		if isLike {
			l.Val = likeStr
			like := "notLike"
			if isInsensitive {
				like = "notILike"
			}
			clause, err = l.doLike(like)
		} else {
			clause = sql.Eq(&sqlMatch{
				col:     sql.NewRawObject("string"),
				pattern: l.Val,
			}, sql.NewIntVal(1))
		}
		break
	case "|>":
		likeOp := l.patternMatch(l.Val)
		clause = sql.BinaryLogicalOp("LIKE", sql.NewRawObject("string"), sql.NewStringVal(likeOp))
		break
	default:
		err = &shared.NotSupportedError{fmt.Sprintf("%s not supported", l.Op)}
	}

	if err != nil {
		return nil, err
	}
	return req.AndWhere(clause), nil
}

func (l *LineFilterPlanner) doLike(likeOp string) (sql.SQLCondition, error) {
	enqVal, err := l.enquoteStr(l.Val)
	if err != nil {
		return nil, err
	}
	enqVal = strings.Trim(enqVal, `'`)
	enqVal = strings.Replace(enqVal, "%", "\\%", -1)
	enqVal = strings.Replace(enqVal, "_", "\\_", -1)
	return sql.Eq(
		sql.NewRawObject(fmt.Sprintf("%s(samples.string, '%%%s%%')", likeOp, enqVal)), sql.NewIntVal(1),
	), nil
}

func (l *LineFilterPlanner) patternMatch(match string) string {
	var placeholdes []int
	start := 0
	for true {
		i := strings.Index(match[start:], "<_>")
		if i < 0 {
			break
		}
		if i == 0 {
			placeholdes = append(placeholdes, i+start)
			start += i + 3
			continue
		}
		j := i - 1
		for ; j >= 0 && match[start+j] == '\\'; j-- {
		}
		if (i-j-1)%2 != 0 {
			start += i + 3
			continue
		}
		placeholdes = append(placeholdes, i+start)
		start += i + 3
	}
	likeOp := ""
	start = 0
	for _, i := range placeholdes {
		likeOp += fmt.Sprintf("%s%%", match[start:i])
		start = i + 3
	}
	return likeOp
}

func (l *LineFilterPlanner) enquoteStr(str string) (string, error) {
	return sql.NewStringVal(str).String(&sql.Ctx{
		Params: map[string]sql.SQLObject{},
		Result: map[string]sql.SQLObject{},
	})
}

func (l *LineFilterPlanner) re2Like() (string, bool, bool) {
	exp, err := syntax.Parse(l.Val, syntax.PerlX)
	if err != nil {
		return "", false, false
	}
	if exp.Op != syntax.OpLiteral || exp.Flags & ^(syntax.PerlX|syntax.FoldCase) != 0 {
		return "", false, false
	}
	return string(exp.Rune), exp.Flags&syntax.FoldCase != 0, true
}
