package traceql_transpiler

import (
	"github.com/go-faster/city"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	"github.com/metrico/qryn/reader/model"
	traceql_parser "github.com/metrico/qryn/reader/traceql/parser"
	"k8s.io/utils/strings/slices"
	"sort"
	"strconv"
	"strings"
)

type GroupByProcessor struct {
	main        shared.TraceRequestProcessor
	groupFields []traceql_parser.LabelName

	hashKeys []string
}

func (g *GroupByProcessor) getHashKeys() []string {
	if len(g.groupFields) == 0 {
		return []string{}
	}
	if len(g.hashKeys) > 0 {
		return g.hashKeys
	}
	g.hashKeys = make([]string, len(g.groupFields))
	for i, label := range g.groupFields {
		switch label.Path()[0] {
		case "resource", "span":
			g.hashKeys[i] = strings.Join(label.Path()[1:], ".")
		default:
			g.hashKeys[i] = strings.Join(label.Path(), ".")
		}
	}
	return g.hashKeys
}

func (g *GroupByProcessor) hash(s *model.SpanInfo) uint64 {
	var pairs []string
	hashKeys := g.getHashKeys()
	for i := range s.Attributes {
		idx := slices.Index(hashKeys, s.Attributes[i].Key)
		if idx != -1 {
			pairs = append(pairs,
				strconv.Quote(s.Attributes[i].Key)+"="+strconv.Quote(s.Attributes[i].Value.StringValue))
			s.Attributes[i].Key = "by(" + g.groupFields[idx].String() + ")"
		}
	}
	sort.Strings(pairs)
	return city.CH64([]byte(strings.Join(pairs, ",")))
}

func (g *GroupByProcessor) Process(context *shared.PlannerContext) (chan []model.TraceInfo, error) {
	c, err := g.main.Process(context)
	if err != nil {
		return nil, err
	}
	res := make(chan []model.TraceInfo)
	go func() {
		defer close(res)
		for traces := range c {
			for i := range traces {
				trace := &traces[i]
				spanSetsMap := make(map[uint64][]model.SpanInfo)
				for _, spanSet := range trace.SpanSets {
					for _, span := range spanSet.Spans {
						hashKey := g.hash(&span)
						spanSetsMap[hashKey] = append(spanSetsMap[hashKey], span)
					}
				}
				trace.SpanSets = []model.SpanSet{}
				for _, spanSet := range spanSetsMap {
					trace.SpanSets = append(trace.SpanSets, model.SpanSet{
						Spans:   spanSet,
						Matched: len(spanSet),
					})
				}
			}
			res <- traces
		}
	}()
	return res, nil
}
