package traceql_transpiler

import (
	"fmt"
	"strconv"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
	"github.com/metrico/qryn/v5/reader/utils/logger"
	"github.com/metrico/qryn/v5/reader/utils/sql_select"
)

type TraceQLRequestProcessor struct {
	sqlPlanner shared.SQLRequestPlanner
}

func (t TraceQLRequestProcessor) Process(ctx *shared.PlannerContext) (chan []model.TraceInfo, error) {
	sqlReq, err := t.sqlPlanner.Process(ctx)
	if err != nil {
		return nil, err
	}

	strReq, err := sqlReq.String(&sql_select.Ctx{
		Params: map[string]sql_select.SQLObject{},
		Result: map[string]sql_select.SQLObject{},
	})
	if err != nil {
		return nil, err
	}

	traces, err := t.read(ctx, strReq)
	if err != nil {
		return nil, clickhouse_transpiler.UnresolvedAncestryError(ctx, err)
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

// read runs the search query and reads every trace, so that an error raised
// while ClickHouse streams the result fails the request.
func (t TraceQLRequestProcessor) read(ctx *shared.PlannerContext, strReq string) ([]model.TraceInfo, error) {
	rows, err := ctx.CHDb.QueryCtx(ctx.Ctx, strReq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []model.TraceInfo
	for rows.Next() {
		var (
			traceId           string
			spanIds           []string
			durationsNs       []int64
			timestampsNs      []int64
			startTimeUnixNano int64
			traceDurationMs   int64
			rootServiceName   string
			rootTraceName     string
			spansetSpanIds    [][]string
			matched           []int64
			attrNames         [][]string
			attrTypes         [][]string
			attrValues        [][]float64
			attrTexts         [][]string
			spanAttrKeys      [][]string
			spanAttrVals      [][]string
		)
		err = rows.Scan(&traceId, &spanIds, &durationsNs, &timestampsNs,
			&startTimeUnixNano, &traceDurationMs, &rootServiceName, &rootTraceName,
			&spansetSpanIds, &matched, &attrNames, &attrTypes, &attrValues, &attrTexts,
			&spanAttrKeys, &spanAttrVals)
		if err != nil {
			logger.Error("ERROR[TRP#1]: ", err)
			return nil, err
		}
		spans := make(map[string]model.SpanInfo, len(spanIds))
		for i, spanId := range spanIds {
			span := searchSpan(spanId, durationsNs[i], timestampsNs[i])
			if i < len(spanAttrKeys) {
				addSelected(&span, spanAttrKeys[i], spanAttrVals[i])
			}
			spans[spanId] = span
		}
		trace := model.TraceInfo{
			TraceID:           traceId,
			RootServiceName:   rootServiceName,
			RootTraceName:     rootTraceName,
			StartTimeUnixNano: fmt.Sprintf("%d", startTimeUnixNano),
			DurationMs:        traceDurationMs,
			SpanSets:          make([]model.SpanSet, len(spansetSpanIds)),
		}
		for i, ids := range spansetSpanIds {
			set := model.SpanSet{
				Spans:      make([]model.SpanInfo, 0, len(ids)),
				Matched:    int(matched[i]),
				Attributes: spanSetAttrs(attrNames[i], attrTypes[i], attrValues[i], attrTexts[i]),
			}
			for _, id := range ids {
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
		res = append(res, trace)
	}
	return res, rows.Err()
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
