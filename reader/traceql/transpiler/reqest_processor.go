package traceql_transpiler

import (
	"fmt"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	"github.com/metrico/qryn/reader/model"
	"github.com/metrico/qryn/reader/utils/logger"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

type TraceQLRequestProcessor struct {
	sqlPlanner shared.SQLRequestPlanner
}

func (t TraceQLRequestProcessor) Process(ctx *shared.PlannerContext) (chan []model.TraceInfo, error) {
	sqlReq, err := t.sqlPlanner.Process(ctx)
	if err != nil {
		return nil, err
	}

	var opts []int
	if ctx.IsCluster {
		opts = append(opts, sql.STRING_OPT_INLINE_WITH)
	}

	strReq, err := sqlReq.String(&sql.Ctx{
		Params: map[string]sql.SQLObject{},
		Result: map[string]sql.SQLObject{},
	})

	fmt.Println(strReq)

	rows, err := ctx.CHDb.QueryCtx(ctx.Ctx, strReq)
	if err != nil {
		return nil, err
	}

	res := make(chan []model.TraceInfo)

	go func() {
		defer rows.Close()
		defer close(res)

		for rows.Next() {
			var (
				traceId           string
				spanIds           []string
				durationsNs       []int64
				timestampsNs      []int64
				startTimeUnixNano int64
				traceDurationMs   float64
				rootServiceName   string
				rootTraceName     string
				attrs             [][][]any
			)
			err = rows.Scan(&traceId, &spanIds, &durationsNs, &timestampsNs,
				&startTimeUnixNano, &traceDurationMs, &rootServiceName, &rootTraceName, &attrs)
			if err != nil {
				logger.Error("ERROR[TRP#1]: ", err)
				return
			}
			for i := range durationsNs {
				if durationsNs[i] == timestampsNs[i] {
					durationsNs[i] = -1
				}
			}
			trace := model.TraceInfo{
				TraceID:           traceId,
				RootServiceName:   rootServiceName,
				RootTraceName:     rootTraceName,
				StartTimeUnixNano: fmt.Sprintf("%d", startTimeUnixNano),
				DurationMs:        traceDurationMs,
				SpanSet: model.SpanSet{
					Spans: make([]model.SpanInfo, len(spanIds)),
				},
			}
			for i, spanId := range spanIds {
				trace.SpanSet.Spans[i].SpanID = spanId
				trace.SpanSet.Spans[i].DurationNanos = fmt.Sprintf("%d", durationsNs[i])
				if durationsNs[i] == -1 {
					trace.SpanSet.Spans[i].DurationNanos = "n/a"
				}
				trace.SpanSet.Spans[i].StartTimeUnixNano = fmt.Sprintf("%d", timestampsNs[i])
				trace.SpanSet.Spans[i].Attributes = make([]model.SpanAttr, 0)
				for j, attr := range attrs[i] {
					trace.SpanSet.Spans[i].Attributes = append(trace.SpanSet.Spans[i].Attributes, model.SpanAttr{
						Key: fmt.Sprintf("%s", attr[0]),
					})
					trace.SpanSet.Spans[i].Attributes[j].Value.StringValue = fmt.Sprintf("%s", attr[1])
				}
			}
			trace.SpanSet.Matched = len(trace.SpanSet.Spans)
			trace.SpanSets = []model.SpanSet{trace.SpanSet}
			sortSpans(trace.SpanSet.Spans)
			res <- []model.TraceInfo{trace}
		}
	}()

	return res, nil
}
