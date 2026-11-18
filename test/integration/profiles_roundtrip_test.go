//go:build integration

// Write-read round trip for profiles (M2, #920): the seed fixture's CPU
// profiles are uploaded to /ingest the way the Pyroscope Go SDK uploads them,
// and read back through the Connect API Grafana's Pyroscope data source uses
// (profile types, label names and values, series, flame graph, time series),
// each answer checked against the fixture. See logs_roundtrip_test.go for the
// pinned responses.

package integration

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"testing"
	"time"

	querierv1 "github.com/metrico/qryn/v5/reader/prof"
	typesv1 "github.com/metrico/qryn/v5/reader/prof/types/v1"
	"github.com/metrico/qryn/v5/test/integration/seed"
	"google.golang.org/protobuf/encoding/protojson"
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

	t.Run("series", func(t *testing.T) {
		var resp querierv1.SeriesResponse
		connectCall(t, "Series", &querierv1.SeriesRequest{
			Matchers: []string{fmt.Sprintf("{%s}", runSelector)}, Start: from, End: to,
		}, &resp)
		// One series per pod and sample type: the SDK upload carries both
		// samples/count and cpu/nanoseconds.
		var got, want []string
		for _, ls := range resp.LabelsSet {
			got = append(got, profileSeriesKey(ls.Labels))
			names := map[string]bool{}
			for _, l := range ls.Labels {
				if names[l.Name] {
					t.Errorf("label %q appears twice in series %s", l.Name, profileSeriesKey(ls.Labels))
				}
				names[l.Name] = true
			}
		}
		for _, svc := range fixture.Services {
			for _, pod := range svc.Pods {
				for _, st := range [][2]string{{"samples", "count"}, {"cpu", "nanoseconds"}} {
					want = append(want, fmt.Sprintf("service_name=%s pod=%s %s:%s", svc.Name, pod.Name, st[0], st[1]))
				}
			}
		}
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("series:\n got %q\nwant %q", got, want)
		}
		assertPinned(t, "profiles/series.json", normalizeProfileSeries(run, &resp))
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
				raw, err := protojson.Marshal(graph)
				if err != nil {
					t.Fatal(err)
				}
				var v any
				mustUnmarshal(t, raw, &v)
				assertPinned(t, "profiles/flamegraph_cpu.json", v)
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
		var resp struct {
			Flamebearer struct {
				Names    []string `json:"names"`
				NumTicks int64    `json:"numTicks"`
			} `json:"flamebearer"`
		}
		mustUnmarshal(t, raw, &resp)
		if resp.Flamebearer.NumTicks != total(checkout) {
			t.Errorf("CPU time = %dns, want %dns", resp.Flamebearer.NumTicks, total(checkout))
		}
		for _, frame := range profiles[0].Stacks[0] {
			if !slices.Contains(resp.Flamebearer.Names, frame) {
				t.Errorf("function %q missing from %v", frame, resp.Flamebearer.Names)
			}
		}
		var v any
		mustUnmarshal(t, raw, &v)
		assertPinned(t, "profiles/render_cpu.json", v)
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
		var resp struct {
			LeftTicks  int64 `json:"leftTicks"`
			RightTicks int64 `json:"rightTicks"`
			Metadata   struct {
				Format string `json:"format"`
			} `json:"metadata"`
		}
		mustUnmarshal(t, raw, &resp)
		if want := cpu[checkout][0]; resp.LeftTicks != want {
			t.Errorf("left (first upload) CPU time = %dns, want %dns", resp.LeftTicks, want)
		}
		if want := cpu[checkout][seed.ProfileUploads-1]; resp.RightTicks != want {
			t.Errorf("right (last upload) CPU time = %dns, want %dns", resp.RightTicks, want)
		}
		if resp.Metadata.Format != "double" {
			t.Errorf("format = %q, want double", resp.Metadata.Format)
		}
		var v any
		mustUnmarshal(t, raw, &v)
		assertPinned(t, "profiles/render_diff_cpu.json", v)
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

// profileSeriesKey names a series by its service, pod and sample type.
func profileSeriesKey(labels []*typesv1.LabelPair) string {
	m := map[string]string{}
	for _, l := range labels {
		m[l.Name] = l.Value
	}
	return fmt.Sprintf("service_name=%s pod=%s %s:%s", m["service_name"], m["pod"], m["__sample_type__"], m["__sample_unit__"])
}

// normalizeProfileSeries turns a Series response into label maps with the run
// label value replaced by "RUN", sorted, so it can be compared with its pinned
// copy. Label order within a series is not part of the comparison.
func normalizeProfileSeries(run string, resp *querierv1.SeriesResponse) any {
	n := normalizer{run: run}
	var out []map[string]string
	for _, ls := range resp.LabelsSet {
		m := map[string]string{}
		for _, l := range ls.Labels {
			m[l.Name] = l.Value
		}
		out = append(out, n.labels(m))
	}
	sort.Slice(out, func(i, j int) bool { return labelKey(out[i]) < labelKey(out[j]) })
	return map[string]any{"labelsSet": out}
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
