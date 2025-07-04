package controllerv1

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/metrico/qryn/reader/model"
	"github.com/metrico/qryn/reader/utils/unmarshal"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	v1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
	"net/http"
	"strconv"
	"time"
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
										span.ServiceName,
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
	return
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
	return
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
		defer func() {
			if err := recover(); err != nil {
				println("!!!!!!!! Q:" + params.Q)
				fmt.Printf("Recovered from panic: %v\n", err)

			}
		}()
		t.searchV2(internalCtx, w, r, params)
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
	return
}

func (t *TempoController) searchV2(ctx context.Context, w http.ResponseWriter, r *http.Request,
	params *traceSearchParams) {
	if params.Limit == 0 {
		params.Limit = 20
	}
	println(params.Q)
	ch, err := t.Service.SearchTraceQL(ctx,
		params.Q, params.Limit, params.Start, params.End, params.Step)
	if err != nil {
		PromError(500, err.Error(), w)
		return
	}
	if ch.Metrics != nil {
		t.printMetrics(w, ch)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	w.Write([]byte(`{"traces": [`))
	i := 0
	for traces := range ch.TraceInfo {
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
}

func (t *TempoController) printMetrics(w http.ResponseWriter, ch *model.TraceQLResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	w.Write([]byte(`{"series": [`))
	i := 0
	for serie := range ch.Metrics {
		strSerie, _ := json.Marshal(serie)
		if i != 0 {
			w.Write([]byte(","))
		}
		w.Write(strSerie)
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
	Step        time.Duration
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
	res.Step, err = time.ParseDuration(orDefault(r.URL.Query().Get("step"), "15s"))
	if err != nil {
		return nil, fmt.Errorf("step: %v", err)
	}
	return &res, nil
}

func orDefault(str string, def string) string {
	if str == "" {
		return def
	}
	return str
}

func parseDurationNS(duration string) (int64, error) {
	if duration == "" {
		return 0, nil
	}
	durationNS, err := time.ParseDuration(duration)
	return int64(durationNS), err

}
