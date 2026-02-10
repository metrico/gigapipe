package traceql_transpiler_v2

import (
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/metrico/qryn/v4/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v4/reader/model"
	"github.com/metrico/qryn/v4/reader/utils/logger"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
)

// RequestProcessor processes SQL queries and returns trace results.
type RequestProcessor struct {
	sqlPlanner   shared.SQLRequestPlanner
	selectAttrs  []string
	mostRecent   bool
}

// NewRequestProcessor creates a new request processor.
func NewRequestProcessor(planner shared.SQLRequestPlanner) *RequestProcessor {
	return &RequestProcessor{sqlPlanner: planner}
}

// NewRequestProcessorWithProjection creates a request processor with projection operators.
func NewRequestProcessorWithProjection(planner shared.SQLRequestPlanner, selectAttrs []string, mostRecent bool) *RequestProcessor {
	return &RequestProcessor{
		sqlPlanner:  planner,
		selectAttrs: selectAttrs,
		mostRecent:  mostRecent,
	}
}

// Process executes the query and returns trace results.
func (r *RequestProcessor) Process(ctx *shared.PlannerContext) (chan []model.TraceInfo, error) {
	// Set projection context
	ctx.SelectAttributes = r.selectAttrs
	ctx.MostRecent = r.mostRecent

	sqlReq, err := r.sqlPlanner.Process(ctx)
	if err != nil {
		return nil, err
	}

	strReq, err := sqlReq.String(&sql.Ctx{
		Params: map[string]sql.SQLObject{},
		Result: map[string]sql.SQLObject{},
	})
	if err != nil {
		return nil, err
	}

	rows, err := ctx.CHDb.QueryCtx(ctx.Ctx, strReq)
	if err != nil {
		return nil, err
	}

	res := make(chan []model.TraceInfo)

	go func() {
		defer rows.Close()
		defer close(res)

		// Collect all traces first to fetch attributes
		var traces []model.TraceInfo
		spanIdToIdx := make(map[string]struct {
			traceIdx int
			spanIdx  int
		})

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
			)
			err = rows.Scan(&traceId, &spanIds, &durationsNs, &timestampsNs,
				&startTimeUnixNano, &traceDurationMs, &rootServiceName, &rootTraceName)
			if err != nil {
				logger.Error("ERROR[TRP_V2#1]: ", err)
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
			traceIdx := len(traces)
			for i, spanId := range spanIds {
				trace.SpanSet.Spans[i].SpanID = spanId
				trace.SpanSet.Spans[i].DurationNanos = fmt.Sprintf("%d", durationsNs[i])
				if durationsNs[i] == -1 {
					trace.SpanSet.Spans[i].DurationNanos = "n/a"
				}
				trace.SpanSet.Spans[i].StartTimeUnixNano = fmt.Sprintf("%d", timestampsNs[i])
				trace.SpanSet.Spans[i].Attributes = make([]model.SpanAttr, 0)

				// Map span ID to its position for attribute lookup
				spanIdToIdx[spanId] = struct {
					traceIdx int
					spanIdx  int
				}{traceIdx, i}
			}
			trace.SpanSet.Matched = len(trace.SpanSet.Spans)
			trace.SpanSets = []model.SpanSet{trace.SpanSet}
			sortSpans(trace.SpanSet.Spans)
			traces = append(traces, trace)
		}

		// Fetch attributes for selected spans if select() is used
		if len(r.selectAttrs) > 0 && len(spanIdToIdx) > 0 {
			r.fetchAttributes(ctx, traces, spanIdToIdx)
		}

		// Send all traces
		for _, trace := range traces {
			res <- []model.TraceInfo{trace}
		}
	}()

	return res, nil
}

// fetchAttributes fetches selected attributes for the given spans.
func (r *RequestProcessor) fetchAttributes(ctx *shared.PlannerContext, traces []model.TraceInfo, spanIdToIdx map[string]struct {
	traceIdx int
	spanIdx  int
}) {
	if len(traces) == 0 || len(r.selectAttrs) == 0 {
		return
	}

	// Collect all span IDs (hex format)
	spanIds := make([]string, 0, len(spanIdToIdx))
	for spanId := range spanIdToIdx {
		spanIds = append(spanIds, spanId)
	}

	// Build query to fetch attributes
	table := ctx.TracesAttrsTable
	if ctx.IsCluster {
		table = ctx.TracesAttrsDistTable
	}

	// Normalize attribute keys (remove scope prefix like span. or resource.)
	attrKeys := make([]string, 0, len(r.selectAttrs))
	for _, attr := range r.selectAttrs {
		key := attr
		// Strip scope prefix
		if strings.HasPrefix(key, "span.") {
			key = key[5:]
		} else if strings.HasPrefix(key, "resource.") {
			key = key[9:]
		} else if strings.HasPrefix(key, ".") {
			key = key[1:]
		}
		attrKeys = append(attrKeys, key)
	}

	// Build IN clause for span IDs (convert hex to binary)
	spanIdBinaries := make([]string, 0, len(spanIds))
	for _, sid := range spanIds {
		spanIdBinaries = append(spanIdBinaries, fmt.Sprintf("unhex('%s')", sid))
	}

	// Build IN clause for keys
	keyQuotes := make([]string, 0, len(attrKeys))
	for _, k := range attrKeys {
		keyQuotes = append(keyQuotes, fmt.Sprintf("'%s'", strings.ReplaceAll(k, "'", "''")))
	}

	query := fmt.Sprintf(`
		SELECT lower(hex(span_id)), key, val
		FROM %s
		WHERE span_id IN (%s)
		  AND key IN (%s)
		  AND date >= '%s'
		  AND date <= '%s'
	`, table,
		strings.Join(spanIdBinaries, ","),
		strings.Join(keyQuotes, ","),
		ctx.From.Format("2006-01-02"),
		ctx.To.Format("2006-01-02"))

	rows, err := ctx.CHDb.QueryCtx(ctx.Ctx, query)
	if err != nil {
		logger.Error("ERROR[TRP_V2#2]: ", err)
		return
	}
	defer rows.Close()

	// Map attributes back to spans
	for rows.Next() {
		var spanId, key, val string
		if err := rows.Scan(&spanId, &key, &val); err != nil {
			logger.Error("ERROR[TRP_V2#3]: ", err)
			continue
		}

		if idx, ok := spanIdToIdx[spanId]; ok {
			// Find the correct key with scope prefix for the response
			attrKey := key
			for _, origKey := range r.selectAttrs {
				stripped := origKey
				if strings.HasPrefix(stripped, "span.") {
					stripped = stripped[5:]
				} else if strings.HasPrefix(stripped, "resource.") {
					stripped = stripped[9:]
				} else if strings.HasPrefix(stripped, ".") {
					stripped = stripped[1:]
				}
				if stripped == key {
					attrKey = origKey
					break
				}
			}

			attr := model.SpanAttr{
				Key: attrKey,
			}
			attr.Value.StringValue = val
			traces[idx.traceIdx].SpanSet.Spans[idx.spanIdx].Attributes = append(
				traces[idx.traceIdx].SpanSet.Spans[idx.spanIdx].Attributes,
				attr,
			)
		}
	}
}

// hexToBytes converts a hex string to bytes.
func hexToBytes(s string) []byte {
	b, _ := hex.DecodeString(s)
	return b
}

// sortSpans sorts spans by start time.
func sortSpans(spans []model.SpanInfo) {
	sort.Slice(spans, func(i, j int) bool {
		return spans[i].StartTimeUnixNano < spans[j].StartTimeUnixNano
	})
}
