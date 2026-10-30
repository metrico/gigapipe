// Package seed declares the data the integration tests write through
// gigapipe's public ingestion APIs. One declaration (Default) lists every
// service and pod, and each signal is derived from it, so query results can be
// checked against numbers known in advance.
//
// Generation is deterministic: the same Fixture always produces the same
// bytes. Run is the value of the RunLabel label on everything written; it keeps
// one test run's data apart from earlier runs against the same ClickHouse.
// Start anchors the timestamps, which are fixed offsets from it.
package seed

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"mime/multipart"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/golang/snappy"
	"github.com/google/pprof/profile"
	"github.com/metrico/qryn/v5/writer/utils/proto/prompb"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// RunLabel is the label carrying Fixture.Run on every series written.
const RunLabel = "m2_run"

// LineInterval is the time between consecutive log lines of a stream.
const LineInterval = 10 * time.Second

// Fixture is one declaration of the data written by a test run.
type Fixture struct {
	Run      string
	Start    time.Time
	Services []Service
}

// Service is one application, sent as the "app" label.
type Service struct {
	Name string
	Pods []Pod
}

// Pod is one instance of a service. For logs it is one stream: Lines lines,
// ErrorLines of them at level "error" and the rest at level "info". For
// metrics it serves RequestsPerScrape requests between two scrapes.
type Pod struct {
	Name              string
	Lines             int
	ErrorLines        int
	RequestsPerScrape int
}

// Default returns the standard fixture used by the integration tests.
func Default(run string, start time.Time) Fixture {
	return Fixture{
		Run:   run,
		Start: start,
		Services: []Service{
			{Name: "checkout", Pods: []Pod{
				{Name: "p1", Lines: 4, ErrorLines: 1, RequestsPerScrape: 3},
				{Name: "p2", Lines: 7, ErrorLines: 3, RequestsPerScrape: 12},
				{Name: "p3", Lines: 1, ErrorLines: 0, RequestsPerScrape: 6},
			}},
			{Name: "payments", Pods: []Pod{
				{Name: "p1", Lines: 5, ErrorLines: 2, RequestsPerScrape: 9},
			}},
		},
	}
}

// LogStream is one stream as sent to gigapipe.
type LogStream struct {
	Labels map[string]string
	Lines  []LogLine
}

// LogLine is one log line. Text is a JSON object with level, msg, latency_ms
// and seq (the line's index in its stream).
type LogLine struct {
	Time  time.Time
	Level string
	Text  string
}

var (
	infoMessages  = []string{"order created", "cart updated", "item reserved", "request served"}
	errorMessages = []string{"payment declined", "inventory timeout", "card rejected"}
)

// Logs returns one stream per pod, in declaration order. Line i of a stream is
// at Start + i*LineInterval.
func (f Fixture) Logs() []LogStream {
	// A fixed seed: the content does not depend on Run or Start, so every run
	// writes the same lines.
	rng := rand.New(rand.NewPCG(920, 2))
	var streams []LogStream
	for _, svc := range f.Services {
		for _, pod := range svc.Pods {
			if pod.ErrorLines > pod.Lines {
				panic(fmt.Sprintf("seed: pod %s/%s has more error lines than lines", svc.Name, pod.Name))
			}
			isError := make([]bool, pod.Lines)
			for i := range pod.ErrorLines {
				isError[i] = true
			}
			rng.Shuffle(len(isError), func(i, j int) { isError[i], isError[j] = isError[j], isError[i] })

			stream := LogStream{Labels: map[string]string{RunLabel: f.Run, "app": svc.Name, "pod": pod.Name}}
			for i := range pod.Lines {
				level, messages := "info", infoMessages
				if isError[i] {
					level, messages = "error", errorMessages
				}
				text, err := json.Marshal(struct {
					Level     string `json:"level"`
					Msg       string `json:"msg"`
					LatencyMs int    `json:"latency_ms"`
					Seq       int    `json:"seq"`
				}{level, messages[rng.IntN(len(messages))], 10 + rng.IntN(990), i})
				if err != nil {
					panic(err)
				}
				stream.Lines = append(stream.Lines, LogLine{
					Time:  f.Start.Add(time.Duration(i) * LineInterval),
					Level: level,
					Text:  string(text),
				})
			}
			streams = append(streams, stream)
		}
	}
	return streams
}

// LokiPushJSON encodes streams as a Loki push request body
// (POST /loki/api/v1/push, application/json).
func LokiPushJSON(streams []LogStream) []byte {
	type pushStream struct {
		Stream map[string]string `json:"stream"`
		Values [][2]string       `json:"values"`
	}
	body := struct {
		Streams []pushStream `json:"streams"`
	}{}
	for _, s := range streams {
		ps := pushStream{Stream: s.Labels}
		for _, l := range s.Lines {
			ps.Values = append(ps.Values, [2]string{strconv.FormatInt(l.Time.UnixNano(), 10), l.Text})
		}
		body.Streams = append(body.Streams, ps)
	}
	out, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}
	return out
}

// ScrapeInterval is the time between consecutive metric samples, as scraped by
// a Prometheus server.
const ScrapeInterval = 15 * time.Second

// Scrapes is the number of samples in each metric series: ten minutes.
const Scrapes = 40

// MetricSeries is one time series as sent to gigapipe. Labels include
// __name__.
type MetricSeries struct {
	Labels  map[string]string
	Samples []MetricSample
}

// MetricSample is one sample of a series.
type MetricSample struct {
	Time  time.Time
	Value float64
}

// Metrics returns two series per pod, in declaration order: the counter
// http_requests_total, which grows by RequestsPerScrape at every scrape, and
// the gauge process_resident_memory_bytes. Sample i is at
// Start + i*ScrapeInterval.
func (f Fixture) Metrics() []MetricSeries {
	rng := rand.New(rand.NewPCG(920, 3))
	var series []MetricSeries
	for _, svc := range f.Services {
		for _, pod := range svc.Pods {
			labels := func(name string) map[string]string {
				return map[string]string{"__name__": name, RunLabel: f.Run, "app": svc.Name, "pod": pod.Name}
			}
			requests := MetricSeries{Labels: labels("http_requests_total")}
			memory := MetricSeries{Labels: labels("process_resident_memory_bytes")}
			counter := float64(1000 + rng.IntN(9000))
			for i := range Scrapes {
				at := f.Start.Add(time.Duration(i) * ScrapeInterval)
				requests.Samples = append(requests.Samples, MetricSample{at, counter + float64(i*pod.RequestsPerScrape)})
				memory.Samples = append(memory.Samples, MetricSample{at, float64(100<<20 + rng.IntN(100<<20))})
			}
			series = append(series, requests, memory)
		}
	}
	return series
}

// PromRemoteWrite encodes series as a Prometheus remote-write request body:
// a snappy-compressed WriteRequest protobuf, labels sorted by name as
// Prometheus sends them.
func PromRemoteWrite(series []MetricSeries) []byte {
	req := &prompb.WriteRequest{}
	for _, s := range series {
		ts := &prompb.TimeSeries{}
		names := make([]string, 0, len(s.Labels))
		for name := range s.Labels {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			ts.Labels = append(ts.Labels, &prompb.Label{Name: name, Value: s.Labels[name]})
		}
		for _, sample := range s.Samples {
			ts.Samples = append(ts.Samples, &prompb.Sample{Value: sample.Value, Timestamp: sample.Time.UnixMilli()})
		}
		req.Timeseries = append(req.Timeseries, ts)
	}
	raw, err := proto.Marshal(req)
	if err != nil {
		panic(err)
	}
	return snappy.Encode(nil, raw)
}

// Trace is one request through the system.
type Trace struct {
	ID    []byte
	Spans []Span
}

// Span is one span of a trace. Service and Pod become the resource attributes
// service.name and k8s.pod.name; Attributes are span attributes.
type Span struct {
	ID         []byte
	ParentID   []byte
	Service    string
	Pod        string
	Name       string
	Kind       tracev1.Span_SpanKind
	Start      time.Time
	End        time.Time
	Attributes map[string]string
}

// Traces returns one checkout request per pod of the first service, each
// calling the first pod of the second service, which writes to its database:
//
//	POST /checkout (first service) -> charge (second service) -> INSERT payments
//
// Trace i starts at Start + i*LineInterval. IDs are derived from Run, so
// traces of different runs never share an ID; everything else is the same in
// every run.
func (f Fixture) Traces() []Trace {
	if len(f.Services) < 2 || len(f.Services[1].Pods) == 0 {
		panic("seed: traces need a second service with a pod")
	}
	rng := rand.New(rand.NewPCG(920, 4))
	frontend, backend := f.Services[0], f.Services[1]
	var traces []Trace
	for i, pod := range frontend.Pods {
		id := func(name string, size int) []byte {
			sum := sha256.Sum256([]byte(fmt.Sprintf("%s/%d/%s", f.Run, i, name)))
			return sum[:size]
		}
		start := f.Start.Add(time.Duration(i) * LineInterval)
		root := time.Duration(100+rng.IntN(200)) * time.Millisecond
		charge := time.Duration(40+rng.IntN(50)) * time.Millisecond
		insert := time.Duration(5+rng.IntN(20)) * time.Millisecond
		traces = append(traces, Trace{ID: id("trace", 16), Spans: []Span{
			{
				ID: id("root", 8), Service: frontend.Name, Pod: pod.Name, Name: "POST /checkout",
				Kind: tracev1.Span_SPAN_KIND_SERVER, Start: start, End: start.Add(root),
				Attributes: map[string]string{"http.request.method": "POST", "http.route": "/checkout"},
			},
			{
				ID: id("charge", 8), ParentID: id("root", 8), Service: backend.Name, Pod: backend.Pods[0].Name, Name: "charge",
				Kind: tracev1.Span_SPAN_KIND_SERVER, Start: start.Add(10 * time.Millisecond), End: start.Add(10*time.Millisecond + charge),
			},
			{
				ID: id("insert", 8), ParentID: id("charge", 8), Service: backend.Name, Pod: backend.Pods[0].Name, Name: "INSERT payments",
				Kind: tracev1.Span_SPAN_KIND_CLIENT, Start: start.Add(20 * time.Millisecond), End: start.Add(20*time.Millisecond + insert),
				Attributes: map[string]string{"db.system": "postgresql", "db.operation.name": "INSERT"},
			},
		}})
	}
	return traces
}

// OTLPTraces encodes traces as an OTLP/HTTP traces request body (protobuf),
// one ResourceSpans per span with the resource attributes service.name,
// k8s.pod.name and RunLabel.
func (f Fixture) OTLPTraces(traces []Trace) []byte {
	str := func(k, v string) *commonv1.KeyValue {
		return &commonv1.KeyValue{Key: k, Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: v}}}
	}
	req := &tracev1.TracesData{}
	for _, tr := range traces {
		for _, sp := range tr.Spans {
			span := &tracev1.Span{
				TraceId: tr.ID, SpanId: sp.ID, ParentSpanId: sp.ParentID, Name: sp.Name, Kind: sp.Kind,
				StartTimeUnixNano: uint64(sp.Start.UnixNano()), EndTimeUnixNano: uint64(sp.End.UnixNano()),
			}
			keys := make([]string, 0, len(sp.Attributes))
			for k := range sp.Attributes {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				span.Attributes = append(span.Attributes, str(k, sp.Attributes[k]))
			}
			req.ResourceSpans = append(req.ResourceSpans, &tracev1.ResourceSpans{
				Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{
					str("service.name", sp.Service), str("k8s.pod.name", sp.Pod), str(RunLabel, f.Run),
				}},
				ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{span}}},
			})
		}
	}
	raw, err := proto.Marshal(req)
	if err != nil {
		panic(err)
	}
	return raw
}

// ProfileInterval is the length of one CPU profile, as uploaded by the
// Pyroscope Go SDK.
const ProfileInterval = 10 * time.Second

// ProfileUploads is the number of consecutive profiles each pod uploads.
const ProfileUploads = 3

// CPUPeriod is the CPU sampling period: 100 samples per second.
const CPUPeriod = 10 * time.Millisecond

// stacks lists, per service position in the declaration, the call stacks its
// CPU profiles sample, leaf first.
var stacks = [][][]string{
	{
		{"main.chargePayment", "main.handleCheckout", "net/http.HandlerFunc.ServeHTTP", "main.main"},
		{"encoding/json.Marshal", "main.handleCheckout", "net/http.HandlerFunc.ServeHTTP", "main.main"},
		{"main.handleCheckout", "net/http.HandlerFunc.ServeHTTP", "main.main"},
	},
	{
		{"database/sql.(*DB).ExecContext", "main.handleCharge", "net/http.HandlerFunc.ServeHTTP", "main.main"},
		{"main.handleCharge", "net/http.HandlerFunc.ServeHTTP", "main.main"},
	},
}

// CPUProfile is one CPU profile upload of a pod. Samples[i] is the number of
// CPU samples taken in Stacks[i].
type CPUProfile struct {
	Service string
	Pod     string
	Start   time.Time
	Stacks  [][]string
	Samples []int64
}

// Profiles returns ProfileUploads consecutive CPU profiles per pod, in
// declaration order, the first starting at Start.
func (f Fixture) Profiles() []CPUProfile {
	if len(f.Services) > len(stacks) {
		panic("seed: no profile stacks declared for every service")
	}
	rng := rand.New(rand.NewPCG(920, 5))
	var out []CPUProfile
	for i, svc := range f.Services {
		for _, pod := range svc.Pods {
			for n := range ProfileUploads {
				p := CPUProfile{Service: svc.Name, Pod: pod.Name, Start: f.Start.Add(time.Duration(n) * ProfileInterval), Stacks: stacks[i]}
				for range stacks[i] {
					p.Samples = append(p.Samples, int64(5+rng.IntN(60)))
				}
				out = append(out, p)
			}
		}
	}
	return out
}

// PyroscopeIngest returns the query string and multipart body the Pyroscope Go
// SDK sends to /ingest for p: a gzipped pprof CPU profile with samples/count
// and cpu/nanoseconds values, and the labels RunLabel and pod.
func (f Fixture) PyroscopeIngest(p CPUProfile) (url.Values, string, []byte) {
	prof := &profile.Profile{
		SampleType:    []*profile.ValueType{{Type: "samples", Unit: "count"}, {Type: "cpu", Unit: "nanoseconds"}},
		PeriodType:    &profile.ValueType{Type: "cpu", Unit: "nanoseconds"},
		Period:        int64(CPUPeriod),
		TimeNanos:     p.Start.UnixNano(),
		DurationNanos: int64(ProfileInterval),
	}
	locations := map[string]*profile.Location{}
	location := func(name string) *profile.Location {
		if l, ok := locations[name]; ok {
			return l
		}
		id := uint64(len(locations) + 1)
		fn := &profile.Function{ID: id, Name: name, SystemName: name, Filename: "main.go"}
		l := &profile.Location{ID: id, Line: []profile.Line{{Function: fn, Line: int64(10 * id)}}}
		locations[name] = l
		prof.Function = append(prof.Function, fn)
		prof.Location = append(prof.Location, l)
		return l
	}
	for i, stack := range p.Stacks {
		sample := &profile.Sample{Value: []int64{p.Samples[i], p.Samples[i] * int64(CPUPeriod)}}
		for _, frame := range stack {
			sample.Location = append(sample.Location, location(frame))
		}
		prof.Sample = append(prof.Sample, sample)
	}
	var pprof bytes.Buffer
	if err := prof.Write(&pprof); err != nil {
		panic(err)
	}

	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	if err := w.SetBoundary(strings.Repeat("-", 8) + "seed"); err != nil {
		panic(err)
	}
	part, err := w.CreateFormFile("profile", "profile.pprof")
	if err != nil {
		panic(err)
	}
	if _, err := part.Write(pprof.Bytes()); err != nil {
		panic(err)
	}
	if err := w.Close(); err != nil {
		panic(err)
	}

	query := url.Values{
		"name":            {fmt.Sprintf("%s{%s=%s,pod=%s}", p.Service, RunLabel, f.Run, p.Pod)},
		"from":            {strconv.FormatInt(p.Start.UnixNano(), 10)},
		"until":           {strconv.FormatInt(p.Start.Add(ProfileInterval).UnixNano(), 10)},
		"spyName":         {"gospy"},
		"sampleRate":      {strconv.Itoa(int(time.Second / CPUPeriod))},
		"units":           {"samples"},
		"aggregationType": {"sum"},
	}
	return query, w.FormDataContentType(), body.Bytes()
}
