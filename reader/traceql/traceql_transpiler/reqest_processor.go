package traceql_transpiler

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/ext"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	traceql_ir "github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
	"github.com/metrico/qryn/v5/reader/utils/logger"
	"github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// TraceQLRequestProcessor runs a search plan: Search, then Data over Search's
// ids sent as the external table clickhouse_transpiler.SearchResultTable.
type TraceQLRequestProcessor struct {
	plan *clickhouse_transpiler.SearchPlan
}

func (t TraceQLRequestProcessor) Process(ctx *shared.PlannerContext) (chan []model.TraceInfo, error) {
	traces, err := t.read(ctx)
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

// render plans planner as SQL.
func render(ctx *shared.PlannerContext, planner shared.SQLRequestPlanner) (string, error) {
	req, err := planner.Process(ctx)
	if err != nil {
		return "", err
	}
	return req.String(&sql_select.Ctx{
		Params: map[string]sql_select.SQLObject{},
		Result: map[string]sql_select.SQLObject{},
	})
}

func (t TraceQLRequestProcessor) read(ctx *shared.PlannerContext) ([]model.TraceInfo, error) {
	searchReq, err := render(ctx, t.plan.Search)
	if err != nil {
		return nil, err
	}
	spansets, table, err := readSearch(ctx, searchReq)
	if err != nil || len(spansets) == 0 {
		return nil, err
	}
	dataReq, err := render(ctx, t.plan.Data)
	if err != nil {
		return nil, err
	}
	return readData(ctx, dataReq, table, spansets)
}

// readSearch runs the Search statement. It returns each listed trace's
// spansets by hex trace id, and the ids as the external table
// clickhouse_transpiler.SearchResultTable.
func readSearch(ctx *shared.PlannerContext, strReq string) (map[string][]searchSpanset, *ext.Table, error) {
	cols := make([]func(*ext.Table) error, len(clickhouse_transpiler.SearchResultColumns))
	for i, c := range clickhouse_transpiler.SearchResultColumns {
		cols[i] = ext.Column(c.Name, column.Type(c.Type))
	}
	table, err := ext.NewTable(clickhouse_transpiler.SearchResultTable, cols...)
	if err != nil {
		return nil, nil, err
	}
	rows, err := ctx.CHDb.QueryCtx(ctx.Ctx, strReq)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	res := map[string][]searchSpanset{}
	for rows.Next() {
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
			return nil, nil, err
		}
		if err := table.Append(traceId, spanIds); err != nil {
			return nil, nil, err
		}
		sets := make([]searchSpanset, len(spansetSpanIds))
		for i, ids := range spansetSpanIds {
			sets[i] = searchSpanset{
				spanIds:    ids,
				matched:    int(matched[i]),
				attributes: spanSetAttrs(attrNames[i], attrTypes[i], attrValues[i], attrTexts[i]),
			}
		}
		res[hex.EncodeToString([]byte(traceId))] = sets
	}
	return res, table, rows.Err()
}

// searchSpanset is a spanset of a Search row: its listed span ids in hex.
type searchSpanset struct {
	spanIds    []string
	matched    int
	attributes []model.SpanSetAttr
}

// readData runs the Data statement with table attached and reads every
// trace, with its spansets, so that an error raised while ClickHouse streams
// the result fails the request.
func readData(ctx *shared.PlannerContext, strReq string, table *ext.Table,
	spansets map[string][]searchSpanset) ([]model.TraceInfo, error) {
	rows, err := ctx.CHDb.QueryCtx(clickhouse.Context(ctx.Ctx, clickhouse.WithExternalTable(table)), strReq)
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
			spanAttrKeys      [][]string
			spanAttrVals      [][]string
		)
		err = rows.Scan(&traceId, &spanIds, &durationsNs, &timestampsNs,
			&startTimeUnixNano, &traceDurationMs, &rootServiceName, &rootTraceName,
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
