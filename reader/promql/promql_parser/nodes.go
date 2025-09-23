package promql_parser

import (
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

type Expr struct {
	Expr        parser.Expr
	Substitutes map[string]Substitute
}

type Substitute struct {
	MetricName string
	Node       parser.Node
	Request    shared.SQLRequestPlanner
}

const (
	TPVectorSelector = 0
	TPLabelMatcher   = 1
)

type Node interface {
	GetNodeType() int
}

type VectorSelector struct {
	node *parser.VectorSelector
}

func (v *VectorSelector) GetNodeType() int {
	return TPVectorSelector
}

func (v *VectorSelector) GetLabelMatchers() []*LabelMatcher {
	res := make([]*LabelMatcher, len(v.node.LabelMatchers))
	for i, v := range v.node.LabelMatchers {
		res[i] = &LabelMatcher{
			Node: v,
		}
	}
	return res
}

type LabelMatcher struct {
	Node *labels.Matcher
}

func (l *LabelMatcher) GetNodeType() int {
	return TPLabelMatcher
}

func (l *LabelMatcher) GetOp() string {
	switch l.Node.Type {
	case labels.MatchEqual:
		return "="
	case labels.MatchNotEqual:
		return "!="
	case labels.MatchRegexp:
		return "=~"
	}
	//case labels.MatchNotRegexp:
	return "!~"
}

func (l *LabelMatcher) GetLabel() string {
	return l.Node.Name
}

func (l *LabelMatcher) GetVal() string {
	return l.Node.Value
}
