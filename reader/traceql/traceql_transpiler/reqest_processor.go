package traceql_transpiler

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
	"github.com/metrico/qryn/v5/reader/utils/logger"
)

// TraceQLRequestProcessor runs a search plan: its ids statement, then Data
// over the ids, skipping Data when there are none.
type TraceQLRequestProcessor struct {
	plan *clickhouse_transpiler.SearchPlan
}

func (t TraceQLRequestProcessor) Process(ctx *shared.PlannerContext) (chan []model.TraceInfo, error) {
	traces, err := t.read(ctx)
	if err != nil {
		return nil, err
	}
	res := make(chan []model.TraceInfo)
	go func() {
		defer close(res)
		for _, trace := range traces {
			res <- []model.TraceInfo{trace}
		}
	}()
	return res, nil
}

// read reads every trace with its spansets before returning.
func (t TraceQLRequestProcessor) read(ctx *shared.PlannerContext) ([]model.TraceInfo, error) {
	spansets := map[string][]searchSpanset{}
	idsScan := func(rows *sql.Rows) (string, []string, error) {
		return scanSearch(rows, spansets)
	}
	var res []model.TraceInfo
	err := clickhouse_transpiler.Query(ctx, t.plan.Ids, idsScan, t.plan.Data, clickhouse_transpiler.SkipOnEmptyIds,
		func(rows *sql.Rows) error {
			trace, err := scanTrace(rows, spansets)
			if err != nil {
				logger.Error("ERROR[TRP#1]: ", err)
				return err
			}
			res = append(res, trace)
			return nil
		})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// scanSearch reads a search's ids row, adding the trace's spansets to
// spansets by hex trace id.
func scanSearch(rows *sql.Rows, spansets map[string][]searchSpanset) (string, []string, error) {
	var (
		traceId        string
		spanIds        []string
		spansetSpanIds [][]string
		matched        []int64
		attrNames      [][]string
		attrTypes      [][]string
		attrValues     [][]float64
		attrTexts      [][]string
	)
	if err := rows.Scan(&traceId, &spanIds, &spansetSpanIds, &matched,
		&attrNames, &attrTypes, &attrValues, &attrTexts); err != nil {
		return "", nil, err
	}
	sets := make([]searchSpanset, len(spansetSpanIds))
	for i, ids := range spansetSpanIds {
		sets[i] = searchSpanset{
			spanIds:    ids,
			matched:    int(matched[i]),
			attributes: spanSetAttrs(attrNames[i], attrTypes[i], attrValues[i], attrTexts[i]),
		}
	}
	spansets[hex.EncodeToString([]byte(traceId))] = sets
	return traceId, spanIds, nil
}

// searchSpanset is a spanset of a search's ids row: its listed span ids in hex.
type searchSpanset struct {
	spanIds    []string
	matched    int
	attributes []model.SpanSetAttr
}

// scanTrace reads a Data row: a trace with its spansets.
func scanTrace(rows *sql.Rows, spansets map[string][]searchSpanset) (model.TraceInfo, error) {
	var (
		traceId           string
		spanIds           []string
		durationsNs       []int64
		timestampsNs      []int64
		startTimeUnixNano int64
		traceDurationMs   int64
		rootServiceName   string
		rootTraceName     string
		spanAttrKeys      [][]string
		spanAttrVals      [][]string
	)
	err := rows.Scan(&traceId, &spanIds, &durationsNs, &timestampsNs,
		&startTimeUnixNano, &traceDurationMs, &rootServiceName, &rootTraceName,
		&spanAttrKeys, &spanAttrVals)
	if err != nil {
		return model.TraceInfo{}, err
	}
	spans := make(map[string]model.SpanInfo, len(spanIds))
	for i, spanId := range spanIds {
		span := searchSpan(spanId, durationsNs[i], timestampsNs[i])
		if i < len(spanAttrKeys) {
			addSelected(&span, spanAttrKeys[i], spanAttrVals[i])
		}
		spans[spanId] = span
	}
	sets := spansets[traceId]
	trace := model.TraceInfo{
		TraceID:           traceId,
		RootServiceName:   rootServiceName,
		RootTraceName:     rootTraceName,
		StartTimeUnixNano: fmt.Sprintf("%d", startTimeUnixNano),
		DurationMs:        traceDurationMs,
		SpanSets:          make([]model.SpanSet, len(sets)),
	}
	for i, s := range sets {
		set := model.SpanSet{
			Spans:      make([]model.SpanInfo, 0, len(s.spanIds)),
			Matched:    s.matched,
			Attributes: s.attributes,
		}
		for _, id := range s.spanIds {
			if span, ok := spans[id]; ok {
				set.Spans = append(set.Spans, span)
			}
		}
		sortSpans(set.Spans)
		trace.SpanSets[i] = set
	}
	if len(trace.SpanSets) > 0 {
		trace.SpanSet = trace.SpanSets[0]
	}
	return trace, nil
}

// searchSpan builds a search-result span without selected values.
func searchSpan(spanId string, durationNs, timestampNs int64) model.SpanInfo {
	return model.SpanInfo{
		SpanID:            spanId,
		DurationNanos:     strconv.FormatInt(durationNs, 10),
		StartTimeUnixNano: strconv.FormatInt(timestampNs, 10),
		Attributes:        make([]model.SpanAttr, 0),
	}
}

// addSelected adds a span's selected values: the span name as its name, a
// nested-set value as an int attribute, any other value as a string attribute.
func addSelected(span *model.SpanInfo, keys, vals []string) {
	for i, key := range keys {
		if key == "name" {
			span.Name = vals[i]
			continue
		}
		attr := model.SpanAttr{Key: key}
		if traceql_ir.IsNestedSet(key) {
			attr.Value.IntValue = &vals[i]
		} else {
			attr.Value.StringValue = &vals[i]
		}
		span.Attributes = append(span.Attributes, attr)
	}
}

// spanSetAttrs builds spanset attributes from their names, types, values
// and texts.
func spanSetAttrs(names, types []string, values []float64, texts []string) []model.SpanSetAttr {
	if len(names) == 0 {
		return nil
	}
	res := make([]model.SpanSetAttr, len(names))
	for i, name := range names {
		res[i].Key = name
		switch types[i] {
		case clickhouse_transpiler.AttrInt:
			v := strconv.FormatInt(int64(values[i]), 10)
			res[i].Value.IntValue = &v
		case clickhouse_transpiler.AttrDuration:
			v := time.Duration(int64(values[i])).String()
			res[i].Value.StringValue = &v
		case clickhouse_transpiler.AttrBool:
			v := values[i] != 0
			res[i].Value.BoolValue = &v
		case clickhouse_transpiler.AttrString:
			v := texts[i]
			res[i].Value.StringValue = &v
		case clickhouse_transpiler.AttrNil:
			v := "nil"
			res[i].Value.StringValue = &v
		default:
			v := model.Double(values[i])
			res[i].Value.DoubleValue = &v
		}
	}
	return res
}
