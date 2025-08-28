package logql_parser

import (
	"fmt"
	"reflect"
	"regexp"

	"github.com/alecthomas/participle/v2"
)

func Parse(str string) (*LogQLScript, error) {
	parser, err := participle.Build[LogQLScript](participle.Lexer(LogQLLexerDefinition), participle.UseLookahead(2))
	if err != nil {
		return nil, err
	}
	res, err := parser.ParseString("", str+" ")
	return res, err
}

func ParseSeries(str string) (*LogQLScript, error) {
	promRe := regexp.MustCompile(`^([a-zA-Z_]\w*)\s*($|\{.+$)`)
	promExp := promRe.FindSubmatch([]byte(str))
	if len(promExp) > 0 {
		left := string(promExp[2])
		if len(left) > 2 {
			left = "," + left[1:]
		} else {
			left = "}"
		}
		str = fmt.Sprintf("{__name__=\"%s\"%s", string(promExp[1]), left)
	}
	parser, err := participle.Build[LogQLScript](participle.Lexer(LogQLLexerDefinition), participle.UseLookahead(2))
	if err != nil {
		return nil, err
	}
	res, err := parser.ParseString("", str+" ")
	return res, err
}

func FindFirst[T any](node any) *T {
	if n, ok := node.(*T); ok {
		return n
	}
	if node == nil || (reflect.ValueOf(node).Kind() == reflect.Ptr && reflect.ValueOf(node).IsNil()) {
		return nil
	}
	switch _node := node.(type) {
	case *LogQLScript:
		return findFirstIn[T](_node.StrSelector, _node.LRAOrUnwrap, _node.AggOperator,
			_node.TopK, _node.QuantileOverTime)
	case *StrSelector:
		var children []any
		for _, c := range _node.Pipelines {
			children = append(children, &c)
		}
		for _, c := range _node.StrSelCmds {
			children = append(children, &c)
		}
		return findFirstIn[T](children...)
	case *StrSelectorPipeline:
		return findFirstIn[T](_node.LineFilter, _node.LabelFilter,
			_node.Parser, _node.LineFormat, _node.LabelFormat, _node.Unwrap, _node.Drop)
	case *LabelFilter:
		return findFirstIn[T](_node.Head, _node.Tail)
	case *Head:
		return findFirstIn[T](_node.SimpleHead, _node.ComplexHead)
	case *SimpleLabelFilter:
		return findFirstIn[T](&_node.Label, _node.StrVal)
	case *Parser:
		var children []any
		for _, c := range _node.ParserParams {
			children = append(children, &c)
		}
		return findFirstIn[T](children...)
	case *ParserParam:
		return findFirstIn[T](_node.Label, &_node.Val)
	case *LineFormat:
		return findFirstIn[T](&_node.Val)
	case *LabelFormat:
		return findFirstIn[T](_node.LabelFormatOps)
	case *LabelFormatOp:
		return findFirstIn[T](&_node.Label, _node.LabelVal, _node.ConstVal)
	case *Unwrap:
		return findFirstIn[T](&_node.Label)
	case *Drop:
		var children []any
		for _, c := range _node.Params {
			children = append(children, &c)
		}
		return findFirstIn[T](children...)
	case *DropParam:
		return findFirstIn[T](_node.Val, &_node.Label)
	case *LRAOrUnwrap:
		return findFirstIn[T](&_node.StrSel, _node.ByOrWithoutPrefix, _node.ByOrWithoutSuffix)
	case *ByOrWithout:
		var labels []any
		for _, l := range _node.Labels {
			labels = append(labels, &l)
		}
		return findFirstIn[T](labels...)
	case *AggOperator:
		return findFirstIn[T](&_node.LRAOrUnwrap, _node.ByOrWithoutPrefix, _node.ByOrWithoutSuffix, _node.Comparison)
	case *TopK:
		return findFirstIn[T](_node.AggOperator, _node.LRAOrUnwrap, _node.QuantileOverTime, _node.Comparison)
	case *QuantileOverTime:
		return findFirstIn[T](&_node.StrSel, _node.Comparison, _node.ByOrWithoutPrefix, _node.ByOrWithoutSuffix)
	}
	return nil
}

func findFirstIn[T any](node ...any) *T {
	for _, n := range node {
		res := FindFirst[T](n)
		if res != nil {
			return res
		}
	}
	return nil
}
