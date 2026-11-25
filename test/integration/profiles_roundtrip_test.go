//go:build integration

// Write-read round trip for profiles (M2, #920): the seed fixture's CPU
// profiles are uploaded to /ingest the way the Pyroscope Go SDK uploads them,
// and read back through the Connect API Grafana's Pyroscope data source uses
// (profile types, label names and values, flame graph, time series), each
// answer checked against the fixture. See logs_roundtrip_test.go for the
// pinned responses.

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	querierv1 "github.com/metrico/qryn/v5/reader/prof"
	typesv1 "github.com/metrico/qryn/v5/reader/prof/types/v1"
	"github.com/metrico/qryn/v5/test/integration/seed"
	"google.golang.org/protobuf/proto"
)

const (
	cpuNanosType   = "process_cpu:cpu:nanoseconds:cpu:nanoseconds"
	cpuSamplesType = "process_cpu:samples:count:cpu:nanoseconds"
)

func TestProfilesRoundTrip(t *testing.T) {
	run := fmt.Sprintf("profiles-%d", time.Now().UnixNano())
	start := time.Now().Add(-10 * time.Minute).Truncate(time.Minute)
	fixture := seed.Default(run, start)
	profiles := fixture.Profiles()
	from, to := start.Add(-time.Minute).UnixMilli(), start.Add(5*time.Minute).UnixMilli()
	runSelector := fmt.Sprintf("%s=%q", seed.RunLabel, run)

	// cpu[service][upload] is the CPU time in nanoseconds the fixture records.
	cpu := map[string][]int64{}
	samples := map[string]int64{}
	for i, p := range profiles {
		if cpu[p.Service] == nil {
			cpu[p.Service] = make([]int64, seed.ProfileUploads)
		}
		for _, n := range p.Samples {
			cpu[p.Service][i%seed.ProfileUploads] += n * int64(seed.CPUPeriod)
			samples[p.Service] += n
		}
		query, contentType, body := fixture.PyroscopeIngest(p)
		postIngest(t, query.Encode(), contentType, body)
	}
	total := func(service string) int64 {
		var sum int64
		for _, v := range cpu[service] {
			sum += v
		}
		return sum
	}
	var all int64
	for svc := range cpu {
		all += total(svc)
	}
	waitForProfiles(t, fmt.Sprintf("{%s}", runSelector), from, to, all)

	t.Run("profile types include CPU", func(t *testing.T) {
		var resp querierv1.ProfileTypesResponse
		connectCall(t, "ProfileTypes", &querierv1.ProfileTypesRequest{Start: from, End: to}, &resp)
		var got []string
		for _, pt := range resp.ProfileTypes {
			got = append(got, pt.ID)
		}
		for _, want := range []string{cpuNanosType, cpuSamplesType} {
			if !slices.Contains(got, want) {
				t.Errorf("profile types %v do not include %q", got, want)
			}
		}
	})

	t.Run("label names", func(t *testing.T) {
		var resp typesv1.LabelNamesResponse
		connectCall(t, "LabelNames", &typesv1.LabelNamesRequest{
			Matchers: []string{fmt.Sprintf("{%s}", runSelector)}, Start: from, End: to,
		}, &resp)
		got := slices.Clone(resp.Names)
		slices.Sort(got)
		want := []string{seed.RunLabel, "pod", "service_name"}
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("label names = %v, want %v", got, want)
		}
	})

	t.Run("service names", func(t *testing.T) {
		var resp typesv1.LabelValuesResponse
		connectCall(t, "LabelValues", &typesv1.LabelValuesRequest{
			Name: "service_name", Matchers: []string{fmt.Sprintf("{%s}", runSelector)}, Start: from, End: to,
		}, &resp)
		got := slices.Clone(resp.Names)
		slices.Sort(got)
		var want []string
		for _, svc := range fixture.Services {
			want = append(want, svc.Name)
		}
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("service names = %v, want %v", got, want)
		}
	})

	for _, svc := range fixture.Services {
		selector := fmt.Sprintf("{%s, service_name=%q}", runSelector, svc.Name)

		t.Run("flame graph of "+svc.Name, func(t *testing.T) {
			graph := mergeStacktraces(t, cpuNanosType, selector, from, to)
			if graph.Total != total(svc.Name) {
				t.Errorf("CPU time = %dns, want %dns", graph.Total, total(svc.Name))
			}
			// Every function the service's stacks sample must appear.
			for i, p := range profiles {
				if p.Service != svc.Name || i%seed.ProfileUploads != 0 {
					continue
				}
				for _, stack := range p.Stacks {
					for _, frame := range stack {
						if !slices.Contains(graph.Names, frame) {
							t.Errorf("function %q missing from the flame graph names %v", frame, graph.Names)
						}
					}
				}
			}
			if svc.Name == fixture.Services[0].Name {
				levels := make([][]int64, len(graph.Levels))
				for i, l := range graph.Levels {
					levels[i] = l.Values
				}
				assertPinned(t, "profiles/flamegraph_cpu.json", struct {
					Total   int64        `json:"total"`
					MaxSelf int64        `json:"maxSelf"`
					Stacks  []flameStack `json:"stacks"`
				}{graph.Total, graph.MaxSelf, singleStacks(t, graph.Names, levels)})
			}
		})

		t.Run("CPU samples of "+svc.Name, func(t *testing.T) {
			if got := mergeStacktraces(t, cpuSamplesType, selector, from, to).Total; got != samples[svc.Name] {
				t.Errorf("samples = %d, want %d", got, samples[svc.Name])
			}
		})
	}

	// The legacy Pyroscope HTTP API: render merges every profile in the range
	// into one flame graph, render-diff compares two ranges.
	checkout := fixture.Services[0].Name
	renderQuery := fmt.Sprintf("%s{%s, service_name=%q}", cpuNanosType, runSelector, checkout)

	t.Run("render merges every upload of "+checkout, func(t *testing.T) {
		raw := apiGet(t, "/pyroscope/render", url.Values{
			"query": {renderQuery},
			"from":  {strconv.FormatInt(from, 10)},
			"until": {strconv.FormatInt(to, 10)},
		})
		resp := decodeRender(t, raw)
		if resp.Flamebearer.NumTicks != total(checkout) {
			t.Errorf("CPU time = %dns, want %dns", resp.Flamebearer.NumTicks, total(checkout))
		}
		for _, frame := range profiles[0].Stacks[0] {
			if !slices.Contains(resp.Flamebearer.Names, frame) {
				t.Errorf("function %q missing from %v", frame, resp.Flamebearer.Names)
			}
		}
		assertPinned(t, "profiles/render_cpu.json", resp.normalize(t, singleStacks(t, resp.Flamebearer.Names, resp.Flamebearer.Levels)))
	})

	t.Run("render-diff compares the first and the last upload of "+checkout, func(t *testing.T) {
		last := start.Add((seed.ProfileUploads - 1) * seed.ProfileInterval)
		raw := apiGet(t, "/pyroscope/render-diff", url.Values{
			"leftQuery":  {renderQuery},
			"leftFrom":   {strconv.FormatInt(start.Add(-5*time.Second).UnixMilli(), 10)},
			"leftUntil":  {strconv.FormatInt(start.Add(5*time.Second).UnixMilli(), 10)},
			"rightQuery": {renderQuery},
			"rightFrom":  {strconv.FormatInt(last.Add(-5*time.Second).UnixMilli(), 10)},
			"rightUntil": {strconv.FormatInt(last.Add(5*time.Second).UnixMilli(), 10)},
		})
		resp := decodeRender(t, raw)
		if want := cpu[checkout][0]; resp.LeftTicks != want {
			t.Errorf("left (first upload) CPU time = %dns, want %dns", resp.LeftTicks, want)
		}
		if want := cpu[checkout][seed.ProfileUploads-1]; resp.RightTicks != want {
			t.Errorf("right (last upload) CPU time = %dns, want %dns", resp.RightTicks, want)
		}
		if resp.Metadata.Format != "double" {
			t.Errorf("format = %q, want double", resp.Metadata.Format)
		}
		assertPinned(t, "profiles/render_diff_cpu.json", resp.normalize(t, diffStacks(t, resp.Flamebearer.Names, resp.Flamebearer.Levels)))
	})

	t.Run("CPU time over time per service", func(t *testing.T) {
		var resp querierv1.SelectSeriesResponse
		connectCall(t, "SelectSeries", &querierv1.SelectSeriesRequest{
			ProfileTypeID: cpuNanosType,
			LabelSelector: fmt.Sprintf("{%s}", runSelector),
			Start:         start.UnixMilli(),
			End:           start.Add(seed.ProfileUploads * seed.ProfileInterval).UnixMilli(),
			GroupBy:       []string{"service_name"},
			Step:          seed.ProfileInterval.Seconds(),
		}, &resp)
		got := map[string][]string{}
		for _, s := range resp.Series {
			service := ""
			for _, l := range s.Labels {
				if l.Name == "service_name" {
					service = l.Value
				}
			}
			for _, p := range s.Points {
				got[service] = append(got[service], fmt.Sprintf("%d@%d", int64(p.Value), p.Timestamp))
			}
		}
		want := map[string][]string{}
		for svc, uploads := range cpu {
			for n, v := range uploads {
				want[svc] = append(want[svc], fmt.Sprintf("%d@%d", v, start.Add(time.Duration(n)*seed.ProfileInterval).UnixMilli()))
			}
		}
		for svc, points := range want {
			if !slices.Equal(got[svc], points) {
				t.Errorf("%s: points %v, want %v", svc, got[svc], points)
			}
		}
		for svc := range got {
			if _, ok := want[svc]; !ok {
				t.Errorf("unexpected series for service %q", svc)
			}
		}
	})
}

func postIngest(t *testing.T, query, contentType string, body []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, baseURL()+"/ingest?"+query, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("ingest: status %d, body %q", resp.StatusCode, raw)
	}
}

// connectCall makes a Connect unary call with a protobuf body, as Grafana's
// Pyroscope data source does.
func connectCall(t *testing.T, method string, req, resp proto.Message) {
	t.Helper()
	raw, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	r, err := http.NewRequest(http.MethodPost, baseURL()+"/querier.v1.QuerierService/"+method, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Content-Type", "application/proto")
	res, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("%s: status %d, body %q", method, res.StatusCode, body)
	}
	if err := proto.Unmarshal(body, resp); err != nil {
		t.Fatalf("%s: decode response: %v", method, err)
	}
}

// renderResponse is a /pyroscope/render or /pyroscope/render-diff response
// as gigapipe sends it, decoded strictly. Timeline, groups and heatmap are
// always null, so they are kept raw.
type renderResponse struct {
	Flamebearer struct {
		Names    []string  `json:"names"`
		Levels   [][]int64 `json:"levels"`
		NumTicks int64     `json:"numTicks"`
		MaxSelf  int64     `json:"maxSelf"`
	} `json:"flamebearer"`
	Metadata struct {
		Format     string `json:"format"`
		SpyName    string `json:"spyName"`
		SampleRate int64  `json:"sampleRate"`
		Units      string `json:"units"`
		Name       string `json:"name"`
	} `json:"metadata"`
	Timeline   json.RawMessage `json:"timeline"`
	Groups     json.RawMessage `json:"groups"`
	Heatmap    json.RawMessage `json:"heatmap"`
	LeftTicks  int64           `json:"leftTicks"`
	RightTicks int64           `json:"rightTicks"`
}

func decodeRender(t *testing.T, raw []byte) renderResponse {
	t.Helper()
	var resp renderResponse
	strictUnmarshal(t, raw, &resp)
	// Several pinned values are zero, empty or null (leftTicks, rightTicks,
	// spyName, timeline...): a missing key would decode to the same value, so
	// every key is required.
	requireFields(t, raw, resp)
	return resp
}

// requireFields fails when the JSON object raw lacks a key that the struct v
// declares, also in nested structs. Together with strict decoding, which
// rejects keys v doesn't declare, the keys must match exactly.
func requireFields(t *testing.T, raw []byte, v any) {
	t.Helper()
	var check func(path string, raw json.RawMessage, typ reflect.Type)
	check = func(path string, raw json.RawMessage, typ reflect.Type) {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(raw, &obj); err != nil {
			t.Fatalf("%s is not a JSON object: %v", strings.TrimSuffix(path, "."), err)
		}
		for i := range typ.NumField() {
			f := typ.Field(i)
			name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
			if name == "" || name == "-" {
				continue
			}
			value, ok := obj[name]
			if !ok {
				t.Fatalf("field %s%s missing from the response", path, name)
			}
			if f.Type.Kind() == reflect.Struct {
				check(path+name+".", value, f.Type)
			}
		}
	}
	check("", raw, reflect.TypeOf(v))
}

// normalize replaces the flame graph's names and levels, whose order the
// query doesn't define, with its call stacks, and keeps every other field.
func (r renderResponse) normalize(t *testing.T, stacks any) any {
	t.Helper()
	return struct {
		Flamebearer struct {
			Stacks   any   `json:"stacks"`
			NumTicks int64 `json:"numTicks"`
			MaxSelf  int64 `json:"maxSelf"`
		} `json:"flamebearer"`
		Metadata   any             `json:"metadata"`
		Timeline   json.RawMessage `json:"timeline"`
		Groups     json.RawMessage `json:"groups"`
		Heatmap    json.RawMessage `json:"heatmap"`
		LeftTicks  int64           `json:"leftTicks"`
		RightTicks int64           `json:"rightTicks"`
	}{
		Flamebearer: struct {
			Stacks   any   `json:"stacks"`
			NumTicks int64 `json:"numTicks"`
			MaxSelf  int64 `json:"maxSelf"`
		}{stacks, r.Flamebearer.NumTicks, r.Flamebearer.MaxSelf},
		Metadata: r.Metadata, Timeline: r.Timeline, Groups: r.Groups, Heatmap: r.Heatmap,
		LeftTicks: r.LeftTicks, RightTicks: r.RightTicks,
	}
}

// flameStack is one call stack of a flame graph, root first, with its total
// and self values.
type flameStack struct {
	Stack string `json:"stack"`
	Total int64  `json:"total"`
	Self  int64  `json:"self"`
}

// flameDiffStack is one call stack of a diff flame graph with the values of
// both sides.
type flameDiffStack struct {
	Stack      string `json:"stack"`
	LeftTotal  int64  `json:"leftTotal"`
	LeftSelf   int64  `json:"leftSelf"`
	RightTotal int64  `json:"rightTotal"`
	RightSelf  int64  `json:"rightSelf"`
}

func singleStacks(t *testing.T, names []string, levels [][]int64) []flameStack {
	t.Helper()
	var out []flameStack
	for _, n := range flameNodes(t, names, levels, 4) {
		out = append(out, flameStack{n.stack, n.values[1], n.values[2]})
	}
	return out
}

func diffStacks(t *testing.T, names []string, levels [][]int64) []flameDiffStack {
	t.Helper()
	var out []flameDiffStack
	for _, n := range flameNodes(t, names, levels, 7) {
		out = append(out, flameDiffStack{n.stack, n.values[1], n.values[2], n.values[4], n.values[5]})
	}
	return out
}

type flameNode struct {
	stack  string
	x      int64 // start on its level
	values []int64
}

// flameNodes rebuilds the call stacks of a flame graph, sorted by stack.
// Each level lists its nodes as groups of width numbers: the node's offset
// from the end of the previous node on the level, its total, ..., and last
// the index of its name (for a diff, width 7, the first three numbers are the
// left side's and are used for positions). A node's parent is the node one
// level up whose range contains the node's start, so names order and the
// order of siblings don't matter.
func flameNodes(t *testing.T, names []string, levels [][]int64, width int) []flameNode {
	t.Helper()
	var all []flameNode
	var above []flameNode
	for li, level := range levels {
		if len(level)%width != 0 {
			t.Fatalf("flame graph level %d has %d values, not groups of %d", li, len(level), width)
		}
		var here []flameNode
		var x int64
		for j := 0; j < len(level); j += width {
			g := level[j : j+width]
			x += g[0]
			idx := g[width-1]
			if idx < 0 || int(idx) >= len(names) {
				t.Fatalf("flame graph level %d: name index %d out of %d names", li, idx, len(names))
			}
			stack := names[idx]
			if li > 0 {
				parent := -1
				for k, p := range above {
					if p.x <= x && x < p.x+p.values[1] || p.x == x && p.values[1] == 0 {
						parent = k
						break
					}
				}
				if parent < 0 {
					t.Fatalf("flame graph level %d: no parent for %q at %d", li, names[idx], x)
				}
				stack = above[parent].stack + ";" + stack
			}
			n := flameNode{stack: stack, x: x, values: g}
			here = append(here, n)
			all = append(all, n)
			x += g[1]
		}
		above = here
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].stack < all[j].stack })
	return all
}

func mergeStacktraces(t *testing.T, profileType, selector string, from, to int64) *querierv1.FlameGraph {
	t.Helper()
	var resp querierv1.SelectMergeStacktracesResponse
	connectCall(t, "SelectMergeStacktraces", &querierv1.SelectMergeStacktracesRequest{
		ProfileTypeID: profileType, LabelSelector: selector, Start: from, End: to,
	}, &resp)
	if resp.Flamegraph == nil {
		return &querierv1.FlameGraph{}
	}
	return resp.Flamegraph
}

// waitForProfiles polls until the flame graph of the selector adds up to want
// nanoseconds of CPU time, since ingestion is flushed asynchronously. On
// timeout it only logs, so the named checks that follow still run and report
// what is wrong.
func waitForProfiles(t *testing.T, selector string, from, to, want int64) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		got := mergeStacktraces(t, cpuNanosType, selector, from, to).Total
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Logf("after 30s the flame graph totals %dns, want %dns; checking anyway", got, want)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}
