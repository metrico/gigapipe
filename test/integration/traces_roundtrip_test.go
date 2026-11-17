//go:build integration

// Write-read round trip for traces (M2, #920): the seed fixture's traces are
// sent as an OTLP/HTTP request and read back through the Tempo API the way
// Grafana's Tempo data source reads them (trace by ID as protobuf, TraceQL
// search, tag names and values), each answer checked against the fixture. See
// logs_roundtrip_test.go for the pinned responses.

package integration

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/test/integration/seed"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestTracesRoundTrip(t *testing.T) {
	run := fmt.Sprintf("traces-%d", time.Now().UnixNano())
	start := time.Now().Add(-10 * time.Minute).Truncate(time.Minute)
	fixture := seed.Default(run, start)
	traces := fixture.Traces()
	from, to := start.Add(-time.Minute), start.Add(5*time.Minute)
	window := url.Values{"start": {unixSeconds(from)}, "end": {unixSeconds(to)}}
	runQuery := fmt.Sprintf(`{ resource.%s = %q }`, seed.RunLabel, run)

	postOTLPTraces(t, fixture.OTLPTraces(traces))
	waitForTraces(t, runQuery, window, len(traces))

	t.Run("trace by ID returns every span", func(t *testing.T) {
		for i, tr := range traces {
			got := fetchTraceSpans(t, tr.ID, window)
			if len(got) != len(tr.Spans) {
				t.Errorf("trace %d: %d spans, want %d", i, len(got), len(tr.Spans))
			}
			for _, want := range tr.Spans {
				span, ok := got[hex.EncodeToString(want.ID)]
				if !ok {
					t.Errorf("trace %d: span %q missing", i, want.Name)
					continue
				}
				if span.service != want.Service || span.name != want.Name || span.kind != want.Kind ||
					span.parent != hex.EncodeToString(want.ParentID) ||
					span.start != uint64(want.Start.UnixNano()) || span.end != uint64(want.End.UnixNano()) {
					t.Errorf("trace %d span %q:\n got %+v\nwant service %s, kind %v, parent %x, start %d, end %d",
						i, want.Name, span, want.Service, want.Kind, want.ParentID, want.Start.UnixNano(), want.End.UnixNano())
				}
				for k, v := range want.Attributes {
					if span.attributes[k] != v {
						t.Errorf("trace %d span %q: attribute %s = %q, want %q", i, want.Name, k, span.attributes[k], v)
					}
				}
			}
		}
	})

	t.Run("search finds every trace of the run", func(t *testing.T) {
		raw := apiGet(t, "/api/search", withQuery(window, runQuery))
		got := map[string]searchTrace{}
		for _, tr := range decodeSearch(t, raw) {
			got[tr.TraceID] = tr
		}
		if len(got) != len(traces) {
			t.Errorf("%d traces found, want %d", len(got), len(traces))
		}
		for i, tr := range traces {
			id := hex.EncodeToString(tr.ID)
			found, ok := got[id]
			if !ok {
				t.Errorf("trace %d (%s) not found", i, id)
				continue
			}
			root := tr.Spans[0]
			wantDuration := int(root.End.Sub(root.Start) / time.Millisecond)
			if found.RootServiceName != root.Service || found.RootTraceName != root.Name ||
				found.StartTimeUnixNano != strconv.FormatInt(root.Start.UnixNano(), 10) || found.DurationMs != wantDuration {
				t.Errorf("trace %d: got %+v, want root %s %q starting at %d, %dms",
					i, found, root.Service, root.Name, root.Start.UnixNano(), wantDuration)
			}
		}
		assertPinned(t, "traces/search.json", normalizeSearch(t, raw, run, start, traces))
	})

	t.Run("search by service and span name", func(t *testing.T) {
		q := fmt.Sprintf(`{ resource.%s = %q && resource.service.name = %q && name = "charge" }`,
			seed.RunLabel, run, fixture.Services[1].Name)
		var got []string
		for _, tr := range decodeSearch(t, apiGet(t, "/api/search", withQuery(window, q))) {
			got = append(got, tr.TraceID)
			if tr.SpanSet.Matched != 1 {
				t.Errorf("trace %s: %d spans matched, want 1", tr.TraceID, tr.SpanSet.Matched)
			}
		}
		var want []string
		for _, tr := range traces {
			want = append(want, hex.EncodeToString(tr.ID))
		}
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("traces = %v, want %v", got, want)
		}
	})

	t.Run("search by duration", func(t *testing.T) {
		const limit = 200 * time.Millisecond
		q := fmt.Sprintf(`{ resource.%s = %q && duration > 200ms }`, seed.RunLabel, run)
		var got []string
		for _, tr := range decodeSearch(t, apiGet(t, "/api/search", withQuery(window, q))) {
			got = append(got, tr.TraceID)
		}
		var want []string
		for _, tr := range traces {
			for _, sp := range tr.Spans {
				if sp.End.Sub(sp.Start) > limit {
					want = append(want, hex.EncodeToString(tr.ID))
					break
				}
			}
		}
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("traces = %v, want %v", got, want)
		}
	})

	// The tag list is not limited to this run, so it checks that the names
	// written are listed, in whichever scope.
	t.Run("tag names", func(t *testing.T) {
		var resp struct {
			Scopes []struct {
				Tags []string `json:"tags"`
			} `json:"scopes"`
		}
		mustUnmarshal(t, apiGet(t, "/api/v2/search/tags", window), &resp)
		var got []string
		for _, s := range resp.Scopes {
			got = append(got, s.Tags...)
		}
		for _, name := range []string{"service.name", "k8s.pod.name", seed.RunLabel, "http.route", "db.system"} {
			if !slices.Contains(got, name) {
				t.Errorf("tags %v do not include %q", got, name)
			}
		}
	})

	t.Run("service names", func(t *testing.T) {
		var resp struct {
			TagValues []string `json:"tagValues"`
		}
		mustUnmarshal(t, apiGet(t, "/api/search/tag/service.name/values", window), &resp)
		for _, svc := range fixture.Services {
			if !slices.Contains(resp.TagValues, svc.Name) {
				t.Errorf("service names %v do not include %q", resp.TagValues, svc.Name)
			}
		}
	})
}

func withQuery(v url.Values, q string) url.Values {
	out := url.Values{"q": {q}, "limit": {"100"}}
	for k, vals := range v {
		out[k] = vals
	}
	return out
}

func postOTLPTraces(t *testing.T, body []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, baseURL()+"/v1/traces", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("OTLP traces: status %d, body %q", resp.StatusCode, raw)
	}
}

type searchTrace struct {
	TraceID           string `json:"traceID"`
	RootServiceName   string `json:"rootServiceName"`
	RootTraceName     string `json:"rootTraceName"`
	StartTimeUnixNano string `json:"startTimeUnixNano"`
	DurationMs        int    `json:"durationMs"`
	SpanSet           struct {
		Matched int `json:"matched"`
	} `json:"spanSet"`
}

func decodeSearch(t *testing.T, raw []byte) []searchTrace {
	t.Helper()
	var resp struct {
		Traces []searchTrace `json:"traces"`
	}
	mustUnmarshal(t, raw, &resp)
	return resp.Traces
}

// waitForTraces polls until the search returns want traces, since ingestion
// is flushed asynchronously. On timeout it only logs, so the named checks that
// follow still run and report what is wrong.
func waitForTraces(t *testing.T, q string, window url.Values, want int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		got := len(decodeSearch(t, apiGet(t, "/api/search", withQuery(window, q))))
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Logf("after 30s the search returns %d traces, want %d; checking anyway", got, want)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

type fetchedSpan struct {
	service    string
	name       string
	kind       tracev1.Span_SpanKind
	parent     string
	start, end uint64
	attributes map[string]string
}

// fetchTraceSpans gets a trace as protobuf, as Grafana does, and returns its
// spans keyed by hex span ID.
func fetchTraceSpans(t *testing.T, id []byte, window url.Values) map[string]fetchedSpan {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet,
		fmt.Sprintf("%s/api/traces/%s?%s", baseURL(), hex.EncodeToString(id), window.Encode()), nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "application/protobuf")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET trace %x: status %d, body %q", id, resp.StatusCode, raw)
	}
	var td tracev1.TracesData
	if err := proto.Unmarshal(raw, &td); err != nil {
		t.Fatalf("trace %x is not an OTLP TracesData: %v", id, err)
	}
	out := map[string]fetchedSpan{}
	for _, rs := range td.ResourceSpans {
		service := ""
		for _, a := range rs.GetResource().GetAttributes() {
			if a.Key == "service.name" {
				service = a.GetValue().GetStringValue()
			}
		}
		for _, ss := range rs.ScopeSpans {
			for _, sp := range ss.Spans {
				attrs := map[string]string{}
				for _, a := range sp.Attributes {
					attrs[a.Key] = a.GetValue().GetStringValue()
				}
				out[hex.EncodeToString(sp.SpanId)] = fetchedSpan{
					service: service, name: sp.Name, kind: sp.Kind, parent: hex.EncodeToString(sp.ParentSpanId),
					start: sp.StartTimeUnixNano, end: sp.EndTimeUnixNano, attributes: attrs,
				}
			}
		}
	}
	return out
}

// normalizeSearch replaces the run-specific values of a search response so it
// can be compared with its pinned copy: trace IDs become "trace-<index>" in
// fixture order, span IDs "span-<trace>-<index>", and times offsets from the
// fixture start in milliseconds, since spans start within a second of each
// other. Traces and spans are sorted.
func normalizeSearch(t *testing.T, raw []byte, run string, start time.Time, traces []seed.Trace) any {
	t.Helper()
	names := map[string]string{}
	for i, tr := range traces {
		names[hex.EncodeToString(tr.ID)] = fmt.Sprintf("trace-%d", i)
		for j, sp := range tr.Spans {
			names[hex.EncodeToString(sp.ID)] = fmt.Sprintf("span-%d-%d", i, j)
		}
	}
	rename := func(id string) string {
		if n, ok := names[id]; ok {
			return n
		}
		t.Errorf("unexpected ID %s in the search response", id)
		return id
	}
	offset := func(ns string) string {
		v, err := strconv.ParseInt(ns, 10, 64)
		if err != nil {
			t.Fatalf("time %q: %v", ns, err)
		}
		return fmt.Sprintf("%+dms", time.Unix(0, v).Sub(start).Milliseconds())
	}
	type span struct {
		SpanID            string `json:"spanID"`
		StartTimeUnixNano string `json:"startTimeUnixNano"`
		DurationNanos     string `json:"durationNanos"`
		Attributes        []any  `json:"attributes"`
	}
	type spanSet struct {
		Spans   []span `json:"spans"`
		Matched int    `json:"matched"`
	}
	var resp struct {
		Traces []struct {
			TraceID           string    `json:"traceID"`
			RootServiceName   string    `json:"rootServiceName"`
			RootTraceName     string    `json:"rootTraceName"`
			StartTimeUnixNano string    `json:"startTimeUnixNano"`
			DurationMs        int       `json:"durationMs"`
			SpanSet           spanSet   `json:"spanSet"`
			SpanSets          []spanSet `json:"spanSets"`
		} `json:"traces"`
	}
	mustUnmarshal(t, raw, &resp)
	fix := func(s spanSet) spanSet {
		for i := range s.Spans {
			s.Spans[i].SpanID = rename(s.Spans[i].SpanID)
			s.Spans[i].StartTimeUnixNano = offset(s.Spans[i].StartTimeUnixNano)
		}
		sort.Slice(s.Spans, func(i, j int) bool { return s.Spans[i].SpanID < s.Spans[j].SpanID })
		return s
	}
	for i, tr := range resp.Traces {
		tr.TraceID = rename(tr.TraceID)
		tr.StartTimeUnixNano = offset(tr.StartTimeUnixNano)
		tr.SpanSet = fix(tr.SpanSet)
		for j := range tr.SpanSets {
			tr.SpanSets[j] = fix(tr.SpanSets[j])
		}
		resp.Traces[i] = tr
	}
	sort.Slice(resp.Traces, func(i, j int) bool { return resp.Traces[i].TraceID < resp.Traces[j].TraceID })
	return resp
}
