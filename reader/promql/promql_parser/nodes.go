package promql_parser

import (
	"fmt"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

type Expr struct {
	Expr         parser.Expr
	Substitutes  map[string]*Substitute
	substCounter int64
}

// NextSubstituteName returns a fresh, deterministic pushdown-substitute metric
// name for this transpile. It replaces a random suffix so the SQL a query
// lowers to is reproducible across runs (required for golden-SQL pinning).
func (e *Expr) NextSubstituteName() string {
	e.substCounter++
	return fmt.Sprintf("__metric_subst__%d", e.substCounter)
}

type Substitute struct {
	MetricName string
	Notes      SubstituteNotes
	Node       parser.Node
	Request    shared.SQLRequestPlanner
}

type SubstituteNotes struct {
	NeedsLabelsValues bool
	// DropMetricName mirrors prometheus: range functions (rate, increase,
	// *_over_time, ...) return an instant vector without __name__, whereas a
	// bare selector keeps it. Only set for the range-function path.
	DropMetricName bool
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
