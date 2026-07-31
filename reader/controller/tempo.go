package controller

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/metrico/qryn/v4/reader/model"
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
	if len(traceId) < 32 {
		traceId = strings.Repeat("0", 32-len(traceId)) + traceId
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
	if strings.HasSuffix(r.URL.Path, "/json") {
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
		if len(resourceSpans) == 0 {
			http.Error(w, "Not found", http.StatusNotFound)
			return
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
		w.Write(bTraceData)
	default:
		spans := make([]*model.SpanResponse, 0)
		for span := range res {
			spans = append(spans, span)
		}
		if len(spans) == 0 {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"resourceSpans": [{
			"resource":{"attributes":[{"key":"collector","value":{"stringValue":"qryn"}}]},
			"instrumentationLibrarySpans": [{ "spans": [`))
		for i, span := range spans {
			res, err := json.Marshal(unmarshal.SpanToJSONSpan(span.Span, span.ServiceName))
			if err != nil {
				PromError(500, err.Error(), w)
				return
			}
			if i != 0 {
				w.Write([]byte(","))
			}
			w.Write(res)
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

	// Known OpenTelemetry resource attribute prefixes
	resourcePrefixes := []string{
		"service.", "telemetry.", "deployment.", "host.", "os.", "process.",
		"container.", "k8s.", "cloud.", "faas.", "device.", "webengine.",
	}
	resourceExact := map[string]bool{
		"instance": true, "local_endpoint_service_name": true,
	}

	var resourceTags, spanTags []string
	for _, tag := range arrRes {
		isResource := resourceExact[tag]
		if !isResource {
			for _, prefix := range resourcePrefixes {
				if strings.HasPrefix(tag, prefix) {
					isResource = true
					break
				}
			}
		}
		if isResource {
			resourceTags = append(resourceTags, tag)
		} else {
			spanTags = append(spanTags, tag)
		}
	}
	intrinsicTags := []string{"duration", "name", "status", "statusMessage", "kind", "rootName", "rootServiceName", "traceDuration"}

	scopes := []any{}
	if len(resourceTags) > 0 {
		scopes = append(scopes, map[string]any{"name": "resource", "tags": resourceTags})
	}
	if len(spanTags) > 0 {
		scopes = append(scopes, map[string]any{"name": "span", "tags": spanTags})
	}
	scopes = append(scopes, map[string]any{"name": "intrinsic", "tags": intrinsicTags})

	res := map[string]any{"scopes": scopes}

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
	if tag == "status" {
		tag = "otel.status_code"
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
	if tag == "status" {
		tag = "otel.status_code"
	}
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
			fmt.Println(err)
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
	startS, err := strconv.ParseInt(orDefault(r.URL.Query().Get("start"), "0"), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("start: %v", err)
	}
	res.Start = epochToTime(startS)
	if startS == 0 {
		res.Start = time.Now().Add(time.Hour * -6)
	}
	endS, err := strconv.ParseInt(orDefault(r.URL.Query().Get("end"), "0"), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("end: %v", err)
	}
	res.End = epochToTime(endS)
	if endS == 0 {
		res.End = time.Now()
	}
	return &res, nil
}

// epochToTime converts an epoch timestamp to time.Time, handling seconds, milliseconds, and nanoseconds.
func epochToTime(v int64) time.Time {
	switch {
	case v >= 1e18:
		return time.Unix(0, v)
	case v >= 1e12:
		return time.Unix(0, v*int64(time.Millisecond))
	default:
		return time.Unix(v, 0)
	}
}

func orDefault(str string, def string) string {
	if str == "" {
		return def
	}
	return str
}

// MetricsQueryRange handles /api/metrics/query_range and /tempo/api/metrics/query_range.
func (t *TempoController) MetricsQueryRange(w http.ResponseWriter, r *http.Request) {
	internalCtx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}

	req, err := parseMetricsRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := t.Service.MetricsQueryRange(internalCtx, req)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// MetricsQueryInstant handles /api/metrics/query and /tempo/api/metrics/query.
func (t *TempoController) MetricsQueryInstant(w http.ResponseWriter, r *http.Request) {
	internalCtx, err := RunPreRequestPlugins(r)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}

	req, err := parseMetricsRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := t.Service.MetricsQueryInstant(internalCtx, req)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// parseMetricsRequest parses query parameters for metrics endpoints.
func parseMetricsRequest(r *http.Request) (*model.MetricsQueryRequest, error) {
	q := r.URL.Query().Get("q")
	if q == "" {
		q = r.URL.Query().Get("query")
	}
	if q == "" {
		return nil, fmt.Errorf("missing required parameter: q")
	}

	now := time.Now()
	var from, to time.Time

	sinceStr := r.URL.Query().Get("since")
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")

	if startStr != "" {
		fromTS, err := parseTempoTimestamp(startStr)
		if err != nil {
			return nil, fmt.Errorf("invalid start: %w", err)
		}
		from = fromTS
	}
	if endStr != "" {
		toTS, err := parseTempoTimestamp(endStr)
		if err != nil {
			return nil, fmt.Errorf("invalid end: %w", err)
		}
		to = toTS
	}

	if from.IsZero() && to.IsZero() {
		if sinceStr == "" {
			sinceStr = "1h"
		}
		sinceDur, err := parseMetricsDuration(sinceStr)
		if err != nil {
			return nil, fmt.Errorf("invalid since: %w", err)
		}
		to = now
		from = now.Add(-sinceDur)
	} else if from.IsZero() {
		from = to.Add(-time.Hour)
	} else if to.IsZero() {
		to = now
	}

	var step time.Duration
	stepStr := r.URL.Query().Get("step")
	if stepStr != "" {
		stepDur, err := parseMetricsDuration(stepStr)
		if err != nil {
			return nil, fmt.Errorf("invalid step: %w", err)
		}
		step = stepDur
	}
	if step == 0 {
		step = autoStep(from, to)
	}

	return &model.MetricsQueryRequest{
		Query: q,
		From:  from,
		To:    to,
		Step:  step,
	}, nil
}

// parseTempoTimestamp parses a Tempo-style timestamp (unix seconds, nanoseconds, or RFC3339).
func parseTempoTimestamp(s string) (time.Time, error) {
	if !strings.Contains(s, "T") && !strings.Contains(s, "-") {
		n, err := strconv.ParseInt(s, 10, 64)
		if err == nil {
			if n > 1e15 {
				return time.Unix(0, n), nil
			}
			return time.Unix(n, 0), nil
		}
		f, err := strconv.ParseFloat(s, 64)
		if err == nil {
			sec := int64(f)
			nsec := int64((f - float64(sec)) * 1e9)
			return time.Unix(sec, nsec), nil
		}
		return time.Time{}, fmt.Errorf("cannot parse timestamp %q", s)
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("cannot parse timestamp %q: %w", s, err)
	}
	return t, nil
}

// parseMetricsDuration parses a duration string like "15s", "1m", "1h" or plain float seconds.
func parseMetricsDuration(s string) (time.Duration, error) {
	d, err := time.ParseDuration(s)
	if err == nil {
		return d, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err == nil {
		ts := f * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("duration %q overflows", s)
		}
		return time.Duration(ts), nil
	}
	return 0, fmt.Errorf("cannot parse duration %q", s)
}

const (
	metricsMaxPoints  = 5000
	metricsMinStepSec = 1
)

// autoStep computes a reasonable step for the given time range.
func autoStep(from, to time.Time) time.Duration {
	rangeSec := to.Sub(from).Seconds()
	stepSec := rangeSec / float64(metricsMaxPoints)
	if stepSec < metricsMinStepSec {
		stepSec = metricsMinStepSec
	}
	switch {
	case stepSec <= 1:
		return time.Second
	case stepSec <= 5:
		return 5 * time.Second
	case stepSec <= 10:
		return 10 * time.Second
	case stepSec <= 15:
		return 15 * time.Second
	case stepSec <= 30:
		return 30 * time.Second
	case stepSec <= 60:
		return time.Minute
	case stepSec <= 300:
		return 5 * time.Minute
	case stepSec <= 600:
		return 10 * time.Minute
	case stepSec <= 900:
		return 15 * time.Minute
	case stepSec <= 1800:
		return 30 * time.Minute
	case stepSec <= 3600:
		return time.Hour
	default:
		hours := int(math.Ceil(stepSec / 3600))
		return time.Duration(hours) * time.Hour
	}
}
