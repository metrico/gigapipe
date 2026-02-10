package controller

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/metrico/qryn/v4/reader/model"
	"github.com/metrico/qryn/v4/reader/utils/logger"
	"github.com/metrico/qryn/v4/reader/utils/unmarshal"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	v1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

type TempoController struct {
	Controller
	Service model.ITempoService
}

func (t *TempoController) Trace(w http.ResponseWriter, r *http.Request) {
	internalCtx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	traceId := mux.Vars(r)["traceId"]
	if traceId == "" {
		PromError(400, "traceId is required", w)
		return
	}
	strStart := r.URL.Query().Get("start")
	if strStart == "" {
		strStart = "0"
	}
	start, err := strconv.ParseInt(strStart, 10, 64)
	if err != nil {
		start = 0
	}
	strEnd := r.URL.Query().Get("end")
	if strEnd == "" {
		strEnd = "0"
	}
	end, err := strconv.ParseInt(strEnd, 10, 64)
	if err != nil {
		end = 0
	}
	bTraceId := make([]byte, 32)
	_, err = hex.Decode(bTraceId, []byte(traceId))
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	accept := r.Header.Get("Accept")
	if accept == "" {
		accept = "application/json"
	}
	res, err := t.Service.Query(internalCtx, start*1e9, end*1e9, []byte(traceId), accept == "application/protobuf")
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}

	switch accept {
	case "application/protobuf":
		spansByServiceName := make(map[string]*v1.ResourceSpans, 100)
		for span := range res {
			if _, ok := spansByServiceName[span.ServiceName]; !ok {
				spansByServiceName[span.ServiceName] = &v1.ResourceSpans{
					Resource: &resource.Resource{
						Attributes: []*common.KeyValue{
							{
								Key: "service.name",
								Value: &common.AnyValue{
									Value: &common.AnyValue_StringValue{
										StringValue: span.ServiceName,
									},
								},
							},
						},
					},
					ScopeSpans: []*v1.ScopeSpans{
						{Spans: make([]*v1.Span, 0, 10)},
					},
				}
			}
			spansByServiceName[span.ServiceName].ScopeSpans[0].Spans =
				append(spansByServiceName[span.ServiceName].ScopeSpans[0].Spans, span.Span)
			spansByServiceName[span.ServiceName].ScopeSpans[0].Scope = &common.InstrumentationScope{
				Name:    "N/A",
				Version: "v0",
			}
		}

		resourceSpans := make([]*v1.ResourceSpans, 0, 10)
		for _, spans := range spansByServiceName {
			resourceSpans = append(resourceSpans, spans)
		}
		traceData := v1.TracesData{
			ResourceSpans: resourceSpans,
		}
		bTraceData, err := proto.Marshal(&traceData)
		if err != nil {
			PromError(500, err.Error(), w)
			return
		}
		w.Header().Set("Content-Type", "application/protobuf")
		w.WriteHeader(200)
		w.Write(bTraceData)
	default:
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"resourceSpans": [{ 
			"resource":{"attributes":[{"key":"collector","value":{"stringValue":"qryn"}}]}, 
			"instrumentationLibrarySpans": [{ "spans": [`))
		i := 0
		for span := range res {
			res, err := json.Marshal(unmarshal.SpanToJSONSpan(span.Span))
			if err != nil {
				PromError(500, err.Error(), w)
				return
			}
			if i != 0 {
				w.Write([]byte(","))
			}
			w.Write(res)
			i++
		}
		w.Write([]byte("]}]}]}"))
	}
}

func (t *TempoController) Echo(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("echo"))
}

func (t *TempoController) Tags(w http.ResponseWriter, r *http.Request) {
	internalCtx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	cRes, err := t.Service.Tags(internalCtx)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"tagNames": [`))
	i := 0
	for tag := range cRes {
		if i != 0 {
			w.Write([]byte(","))
		}
		w.Write([]byte(strconv.Quote(tag)))
		i++
	}
	w.Write([]byte("]}"))
}

func (t *TempoController) TagsV2(w http.ResponseWriter, r *http.Request) {
	var err error
	internalCtx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}

	q := r.URL.Query().Get("q")
	var timespan [2]time.Time
	for i, req := range [][]any{{"start", time.Unix(0, 0)}, {"end", time.Unix(0, 0)}} {
		strT := r.URL.Query().Get(req[0].(string))
		if strT == "" {
			timespan[i] = req[1].(time.Time)
			continue
		}
		iT, err := strconv.ParseInt(strT, 10, 64)
		if err != nil {
			PromError(400, fmt.Sprintf("Invalid timestamp for %s: %v", req[0].(string), err), w)
			return
		}
		timespan[i] = time.Unix(iT, 0)
	}

	limit := 2000
	if r.URL.Query().Get("limit") != "" {
		limit, err = strconv.Atoi(r.URL.Query().Get("limit"))
		if err != nil || limit <= 0 || limit > 2000 {
			limit = 2000
		}
	}
	var cRes chan string
	if timespan[0].Unix() == 0 {
		cRes, err = t.Service.Tags(internalCtx)
		if err != nil {
			PromError(500, err.Error(), w)
			return
		}
	} else {
		cRes, err = t.Service.TagsV2(internalCtx, q, timespan[0], timespan[1], limit)
		if err != nil {
			PromError(500, err.Error(), w)
			return
		}
	}

	var arrRes []string
	for v := range cRes {
		arrRes = append(arrRes, v)
	}

	res := map[string]any{
		"scopes": []any{
			map[string]any{
				"name": "unscoped",
				"tags": arrRes,
			},
		},
	}

	bRes, err := json.Marshal(res)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(bRes)
}

func (t *TempoController) ValuesV2(w http.ResponseWriter, r *http.Request) {
	var err error
	internalCtx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	q := r.URL.Query().Get("q")
	var timespan [2]time.Time
	for i, req := range [][]any{{"start", time.Unix(0, 0)}, {"end", time.Unix(0, 0)}} {
		strT := r.URL.Query().Get(req[0].(string))
		if strT == "" {
			timespan[i] = req[1].(time.Time)
			continue
		}
		iT, err := strconv.ParseInt(strT, 10, 64)
		if err != nil {
			PromError(400, fmt.Sprintf("Invalid timestamp for %s: %v", req[0].(string), err), w)
			return
		}
		timespan[i] = time.Unix(iT, 0)
	}
	tag := mux.Vars(r)["tag"]

	limit := 2000
	if r.URL.Query().Get("limit") != "" {
		limit, err = strconv.Atoi(r.URL.Query().Get("limit"))
		if err != nil || limit <= 0 || limit > 2000 {
			limit = 2000
		}
	}

	var cRes chan string

	if timespan[0].Unix() == 0 {
		cRes, err = t.Service.Values(internalCtx, tag)
		if err != nil {
			PromError(500, err.Error(), w)
			return
		}
	} else {
		cRes, err = t.Service.ValuesV2(internalCtx, tag, q, timespan[0], timespan[1], limit)
		if err != nil {
			PromError(500, err.Error(), w)
			return
		}
	}

	var arrRes []map[string]string
	for v := range cRes {
		arrRes = append(arrRes, map[string]string{
			"type":  "string",
			"value": v,
		})
	}

	res := map[string]any{"tagValues": arrRes}

	bRes, err := json.Marshal(res)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(bRes)
}

func (t *TempoController) Values(w http.ResponseWriter, r *http.Request) {
	internalCtx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	tag := mux.Vars(r)["tag"]
	cRes, err := t.Service.Values(internalCtx, tag)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	i := 0
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	w.Write([]byte(`{"tagValues": [`))
	for val := range cRes {
		if i != 0 {
			w.Write([]byte(","))
		}
		w.Write([]byte(strconv.Quote(val)))
		i++
	}
	w.Write([]byte(`]}`))
}

func (t *TempoController) Search(w http.ResponseWriter, r *http.Request) {
	internalCtx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	params, err := parseTraceSearchParams(r)
	if err != nil {
		PromError(400, err.Error(), w)
		return
	}

	if params.Q != "" {
		if params.Limit == 0 {
			params.Limit = 20
		}
		ch, err := t.Service.SearchTraceQL(internalCtx,
			params.Q, params.Limit, params.Start, params.End)
		if err != nil {
			PromError(500, err.Error(), w)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		w.Write([]byte(`{"traces": [`))
		i := 0
		for traces := range ch {
			for _, trace := range traces {
				if i != 0 {
					w.Write([]byte(","))
				}
				strTrace, _ := json.Marshal(trace)
				w.Write(strTrace)
				i++
			}
		}
		w.Write([]byte("]}"))
		return
	}

	resChan, err := t.Service.Search(
		internalCtx,
		params.Tags,
		params.MinDuration.Nanoseconds(),
		params.MaxDuration.Nanoseconds(),
		params.Limit,
		params.Start.UnixNano(),
		params.End.UnixNano())
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"traces": [`))
	i := 0
	for trace := range resChan {
		bTrace, err := json.Marshal(trace)
		if err != nil {
			logger.Error("failed to marshal trace: ", err.Error())
			continue
		}
		if i != 0 {
			w.Write([]byte(","))
		}
		w.Write(bTrace)
		i++
	}
	w.Write([]byte("]}"))
}

type traceSearchParams struct {
	Q           string
	Tags        string
	MinDuration time.Duration
	MaxDuration time.Duration
	Limit       int
	Start       time.Time
	End         time.Time
}

func parseTraceSearchParams(r *http.Request) (*traceSearchParams, error) {
	var err error
	res := traceSearchParams{}
	// Tempo parser handles all TraceQL features natively, no normalization needed
	res.Q = r.URL.Query().Get("q")
	res.Tags = r.URL.Query().Get("tags")
	res.MinDuration, err = time.ParseDuration(orDefault(r.URL.Query().Get("minDuration"), "0"))
	if err != nil {
		return nil, fmt.Errorf("minDuration: %v", err)
	}
	res.MaxDuration, err = time.ParseDuration(orDefault(r.URL.Query().Get("maxDuration"), "0"))
	if err != nil {
		return nil, fmt.Errorf("maxDuration: %v", err)
	}
	res.Limit, err = strconv.Atoi(orDefault(r.URL.Query().Get("limit"), "10"))
	if err != nil {
		return nil, fmt.Errorf("limit: %v", err)
	}
	startS, err := strconv.Atoi(orDefault(r.URL.Query().Get("start"), "0"))
	if err != nil {
		return nil, fmt.Errorf("start: %v", err)
	}
	res.Start = time.Unix(int64(startS), 0)
	if startS == 0 {
		res.Start = time.Now().Add(time.Hour * -6)
	}
	endS, err := strconv.Atoi(orDefault(r.URL.Query().Get("end"), "0"))
	if err != nil {
		return nil, fmt.Errorf("end: %v", err)
	}
	res.End = time.Unix(int64(endS), 0)
	if endS == 0 {
		res.End = time.Now()
	}
	return &res, nil
}

func orDefault(str string, def string) string {
	if str == "" {
		return def
	}
	return str
}


// parseStepDuration parses step parameter which can be either:
// - plain integer (seconds): "60"
// - duration string: "28s", "1m", "5m30s"
func parseStepDuration(s string) (time.Duration, error) {
	// First try parsing as plain integer (seconds)
	if seconds, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Duration(seconds) * time.Second, nil
	}
	// Try parsing as Go duration string (e.g., "28s", "1m", "5m30s")
	return time.ParseDuration(s)
}

// MetricsQueryRange handles GET /api/metrics/query_range
func (t *TempoController) MetricsQueryRange(w http.ResponseWriter, r *http.Request) {
	internalCtx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}

	// Tempo API uses "q" parameter, Prometheus uses "query"
	// Note: Tempo parser handles {true && true} natively, no normalization needed
	query := r.URL.Query().Get("q")
	if query == "" {
		query = r.URL.Query().Get("query")
	}
	if query == "" {
		PromError(400, "q parameter is required", w)
		return
	}

	startS, err := strconv.ParseInt(orDefault(r.URL.Query().Get("start"), "0"), 10, 64)
	if err != nil {
		PromError(400, fmt.Sprintf("invalid start: %v", err), w)
		return
	}
	endS, err := strconv.ParseInt(orDefault(r.URL.Query().Get("end"), "0"), 10, 64)
	if err != nil {
		PromError(400, fmt.Sprintf("invalid end: %v", err), w)
		return
	}
	step, err := parseStepDuration(orDefault(r.URL.Query().Get("step"), "60"))
	if err != nil {
		PromError(400, fmt.Sprintf("invalid step: %v", err), w)
		return
	}

	start := time.Unix(startS, 0)
	end := time.Unix(endS, 0)
	if startS == 0 {
		start = time.Now().Add(-time.Hour)
	}
	if endS == 0 {
		end = time.Now()
	}

	result, err := t.Service.MetricsQueryRange(internalCtx, query, start, end, step)
	if err != nil {
		errStr := err.Error()
		// Handle context cancellation (client disconnected)
		if strings.Contains(errStr, "context canceled") || strings.Contains(errStr, "context deadline exceeded") {
			// 499 Client Closed Request (nginx convention)
			w.WriteHeader(499)
			return
		}
		// Return 400 for parse errors and unsupported operations
		if strings.Contains(errStr, "not supported") ||
			strings.Contains(errStr, "parse error") ||
			strings.Contains(errStr, "unsupported") ||
			strings.Contains(errStr, "not a metrics query") {
			PromError(400, errStr, w)
		} else {
			PromError(500, errStr, w)
		}
		return
	}

	// Format response in Tempo native format (for Grafana Tempo datasource)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)

	response := formatTempoRangeResponse(result)
	json.NewEncoder(w).Encode(response)
}

// MetricsQuery handles GET /api/metrics/query (instant query)
func (t *TempoController) MetricsQuery(w http.ResponseWriter, r *http.Request) {
	internalCtx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}

	// Tempo API uses "q" parameter, Prometheus uses "query"
	// Note: Tempo parser handles {true && true} natively, no normalization needed
	query := r.URL.Query().Get("q")
	if query == "" {
		query = r.URL.Query().Get("query")
	}
	if query == "" {
		PromError(400, "q parameter is required", w)
		return
	}

	timeS, err := strconv.ParseInt(orDefault(r.URL.Query().Get("time"), "0"), 10, 64)
	if err != nil {
		PromError(400, fmt.Sprintf("invalid time: %v", err), w)
		return
	}

	ts := time.Unix(timeS, 0)
	if timeS == 0 {
		ts = time.Now()
	}

	result, err := t.Service.MetricsQueryInstant(internalCtx, query, ts)
	if err != nil {
		errStr := err.Error()
		// Handle context cancellation (client disconnected)
		if strings.Contains(errStr, "context canceled") || strings.Contains(errStr, "context deadline exceeded") {
			// 499 Client Closed Request (nginx convention)
			w.WriteHeader(499)
			return
		}
		// Return 400 for parse errors and unsupported operations
		if strings.Contains(errStr, "not supported") ||
			strings.Contains(errStr, "parse error") ||
			strings.Contains(errStr, "unsupported") ||
			strings.Contains(errStr, "not a metrics query") {
			PromError(400, errStr, w)
		} else {
			PromError(500, errStr, w)
		}
		return
	}

	// Format response in Tempo native format (for Grafana Tempo datasource)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)

	response := formatTempoInstantResponse(result)
	json.NewEncoder(w).Encode(response)
}

// Tempo native response format (for Grafana Tempo datasource)
// Matches tempopb.QueryRangeResponse protobuf
type tempoQueryRangeResponse struct {
	Series  []tempoTimeSeries `json:"series"`
	Metrics *tempoMetrics     `json:"metrics,omitempty"`
	Status  int               `json:"status"` // 0=COMPLETE, 1=PARTIAL
	Message string            `json:"message,omitempty"`
}

type tempoMetrics struct {
	InspectedTraces uint32 `json:"inspectedTraces,omitempty"`
	InspectedBytes  uint64 `json:"inspectedBytes,omitempty"`
	InspectedSpans  uint64 `json:"inspectedSpans,omitempty"`
}

type tempoTimeSeries struct {
	Labels  []tempoKeyValue `json:"labels"`
	Samples []tempoSample   `json:"samples"`
}

// tempoKeyValue matches OTEL KeyValue with AnyValue
type tempoKeyValue struct {
	Key   string        `json:"key"`
	Value tempoAnyValue `json:"value"`
}

type tempoAnyValue struct {
	StringValue string `json:"stringValue,omitempty"`
}

type tempoSample struct {
	TimestampMs int64   `json:"timestampMs,string"` // proto int64 serializes as string
	Value       float64 `json:"value"`
}

func formatTempoRangeResponse(result *model.MetricsQueryRangeResult) tempoQueryRangeResponse {
	series := make([]tempoTimeSeries, len(result.Series))

	for i, s := range result.Series {
		samples := make([]tempoSample, len(s.Values))
		for j, v := range s.Values {
			samples[j] = tempoSample{
				TimestampMs: s.Times[j],
				Value:       v,
			}
		}
		series[i] = tempoTimeSeries{
			Labels:  mapToKeyValues(s.Labels),
			Samples: samples,
		}
	}

	return tempoQueryRangeResponse{
		Series: series,
		Status: 0, // COMPLETE
	}
}

func mapToKeyValues(m map[string]string) []tempoKeyValue {
	labels := make([]tempoKeyValue, 0, len(m))
	for k, v := range m {
		labels = append(labels, tempoKeyValue{
			Key:   k,
			Value: tempoAnyValue{StringValue: v},
		})
	}
	return labels
}

func formatTempoInstantResponse(result *model.MetricsQueryResult) tempoQueryRangeResponse {
	series := make([]tempoTimeSeries, len(result.Series))

	for i, s := range result.Series {
		samples := make([]tempoSample, 0, len(s.Values))
		for j, v := range s.Values {
			samples = append(samples, tempoSample{
				TimestampMs: s.Times[j],
				Value:       v,
			})
		}
		series[i] = tempoTimeSeries{
			Labels:  mapToKeyValues(s.Labels),
			Samples: samples,
		}
	}

	return tempoQueryRangeResponse{
		Series: series,
		Status: 0, // COMPLETE
	}
}
