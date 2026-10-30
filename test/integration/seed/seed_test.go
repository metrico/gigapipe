package seed

import (
	"bytes"
	"encoding/json"
	"mime"
	"mime/multipart"
	"slices"
	"testing"
	"time"

	"github.com/google/pprof/profile"
)

var testStart = time.Date(2026, 1, 2, 3, 4, 0, 0, time.UTC)

// TestLogsAreDeterministic checks that generating the same fixture twice gives
// byte-identical push bodies, and that Run only changes the run label.
func TestLogsAreDeterministic(t *testing.T) {
	first := LokiPushJSON(Default("r1", testStart).Logs())
	second := LokiPushJSON(Default("r1", testStart).Logs())
	if !bytes.Equal(first, second) {
		t.Fatal("the same fixture produced different push bodies")
	}

	other := LokiPushJSON(Default("r2", testStart).Logs())
	if !bytes.Equal(bytes.ReplaceAll(other, []byte(`"r2"`), []byte(`"r1"`)), first) {
		t.Fatal("changing Run changed more than the run label")
	}
}

// TestLogsMatchDeclaration checks every stream against its Pod declaration:
// labels, line count, error count, timestamps and line content.
func TestLogsMatchDeclaration(t *testing.T) {
	f := Default("r1", testStart)
	streams := f.Logs()

	i := 0
	for _, svc := range f.Services {
		for _, pod := range svc.Pods {
			s := streams[i]
			i++
			want := map[string]string{RunLabel: "r1", "app": svc.Name, "pod": pod.Name}
			if len(s.Labels) != len(want) {
				t.Errorf("%s/%s: labels = %v, want %v", svc.Name, pod.Name, s.Labels, want)
			}
			for k, v := range want {
				if s.Labels[k] != v {
					t.Errorf("%s/%s: label %s = %q, want %q", svc.Name, pod.Name, k, s.Labels[k], v)
				}
			}
			if len(s.Lines) != pod.Lines {
				t.Fatalf("%s/%s: %d lines, want %d", svc.Name, pod.Name, len(s.Lines), pod.Lines)
			}
			errors := 0
			for n, l := range s.Lines {
				if wantTime := testStart.Add(time.Duration(n) * LineInterval); !l.Time.Equal(wantTime) {
					t.Errorf("%s/%s line %d: time %v, want %v", svc.Name, pod.Name, n, l.Time, wantTime)
				}
				var body struct {
					Level string `json:"level"`
					Msg   string `json:"msg"`
					Seq   int    `json:"seq"`
				}
				if err := json.Unmarshal([]byte(l.Text), &body); err != nil {
					t.Fatalf("%s/%s line %d is not JSON: %v", svc.Name, pod.Name, n, err)
				}
				if body.Level != l.Level || body.Seq != n || body.Msg == "" {
					t.Errorf("%s/%s line %d: body %+v does not match level %q, seq %d", svc.Name, pod.Name, n, body, l.Level, n)
				}
				if l.Level == "error" {
					errors++
				}
			}
			if errors != pod.ErrorLines {
				t.Errorf("%s/%s: %d error lines, want %d", svc.Name, pod.Name, errors, pod.ErrorLines)
			}
		}
	}
	if i != len(streams) {
		t.Errorf("%d streams, want %d", len(streams), i)
	}
}

// TestMetricsAreDeterministic checks that generating the same fixture twice
// gives byte-identical remote-write bodies.
func TestMetricsAreDeterministic(t *testing.T) {
	first := PromRemoteWrite(Default("r1", testStart).Metrics())
	second := PromRemoteWrite(Default("r1", testStart).Metrics())
	if !bytes.Equal(first, second) {
		t.Fatal("the same fixture produced different remote-write bodies")
	}
}

// TestMetricsMatchDeclaration checks every series against its Pod
// declaration: labels, sample times, counter increments and gauge range.
func TestMetricsMatchDeclaration(t *testing.T) {
	f := Default("r1", testStart)
	series := f.Metrics()

	i := 0
	for _, svc := range f.Services {
		for _, pod := range svc.Pods {
			for _, name := range []string{"http_requests_total", "process_resident_memory_bytes"} {
				s := series[i]
				i++
				want := map[string]string{"__name__": name, RunLabel: "r1", "app": svc.Name, "pod": pod.Name}
				if len(s.Labels) != len(want) {
					t.Errorf("%s %s/%s: labels = %v, want %v", name, svc.Name, pod.Name, s.Labels, want)
				}
				for k, v := range want {
					if s.Labels[k] != v {
						t.Errorf("%s %s/%s: label %s = %q, want %q", name, svc.Name, pod.Name, k, s.Labels[k], v)
					}
				}
				if len(s.Samples) != Scrapes {
					t.Fatalf("%s %s/%s: %d samples, want %d", name, svc.Name, pod.Name, len(s.Samples), Scrapes)
				}
				for n, sample := range s.Samples {
					if want := testStart.Add(time.Duration(n) * ScrapeInterval); !sample.Time.Equal(want) {
						t.Errorf("%s %s/%s sample %d: time %v, want %v", name, svc.Name, pod.Name, n, sample.Time, want)
					}
					switch name {
					case "http_requests_total":
						if n > 0 && sample.Value-s.Samples[n-1].Value != float64(pod.RequestsPerScrape) {
							t.Errorf("%s/%s sample %d: counter grew by %v, want %d", svc.Name, pod.Name, n,
								sample.Value-s.Samples[n-1].Value, pod.RequestsPerScrape)
						}
					case "process_resident_memory_bytes":
						if sample.Value < 100<<20 || sample.Value >= 200<<20 {
							t.Errorf("%s/%s sample %d: memory %v outside [100MiB, 200MiB)", svc.Name, pod.Name, n, sample.Value)
						}
					}
				}
			}
		}
	}
	if i != len(series) {
		t.Errorf("%d series, want %d", len(series), i)
	}
}

// TestTracesAreDeterministic checks that the same fixture gives byte-identical
// OTLP bodies, and that another run gets different trace IDs.
func TestTracesAreDeterministic(t *testing.T) {
	f := Default("r1", testStart)
	if !bytes.Equal(f.OTLPTraces(f.Traces()), f.OTLPTraces(f.Traces())) {
		t.Fatal("the same fixture produced different OTLP bodies")
	}
	other := Default("r2", testStart).Traces()
	for i, tr := range f.Traces() {
		if bytes.Equal(tr.ID, other[i].ID) {
			t.Errorf("trace %d has the same ID in runs r1 and r2", i)
		}
	}
}

// TestTracesMatchDeclaration checks one trace per pod of the first service,
// each a chain of three spans with every child inside its parent's time.
func TestTracesMatchDeclaration(t *testing.T) {
	f := Default("r1", testStart)
	traces := f.Traces()
	if len(traces) != len(f.Services[0].Pods) {
		t.Fatalf("%d traces, want %d", len(traces), len(f.Services[0].Pods))
	}
	for i, tr := range traces {
		if len(tr.ID) != 16 || len(tr.Spans) != 3 {
			t.Fatalf("trace %d: ID of %d bytes and %d spans, want 16 and 3", i, len(tr.ID), len(tr.Spans))
		}
		root := tr.Spans[0]
		if root.ParentID != nil || root.Service != f.Services[0].Name || root.Pod != f.Services[0].Pods[i].Name {
			t.Errorf("trace %d root: parent %x, service %s, pod %s", i, root.ParentID, root.Service, root.Pod)
		}
		if want := testStart.Add(time.Duration(i) * LineInterval); !root.Start.Equal(want) {
			t.Errorf("trace %d starts at %v, want %v", i, root.Start, want)
		}
		for n := 1; n < len(tr.Spans); n++ {
			child, parent := tr.Spans[n], tr.Spans[n-1]
			if !bytes.Equal(child.ParentID, parent.ID) {
				t.Errorf("trace %d span %q: parent %x, want %x", i, child.Name, child.ParentID, parent.ID)
			}
			if child.Start.Before(parent.Start) || child.End.After(parent.End) {
				t.Errorf("trace %d span %q is not inside its parent", i, child.Name)
			}
		}
	}
}

// TestProfilesAreDeterministic checks that the same fixture gives
// byte-identical ingest requests.
func TestProfilesAreDeterministic(t *testing.T) {
	f := Default("r1", testStart)
	first, second := f.Profiles(), f.Profiles()
	for i := range first {
		q1, ct1, b1 := f.PyroscopeIngest(first[i])
		q2, ct2, b2 := f.PyroscopeIngest(second[i])
		if q1.Encode() != q2.Encode() || ct1 != ct2 || !bytes.Equal(b1, b2) {
			t.Fatalf("profile %d: the same fixture produced different requests", i)
		}
	}
}

// TestProfilesMatchDeclaration decodes every upload the way the ingest
// endpoint does (multipart field "profile", pprof) and checks it against the
// declaration: one profile per pod and interval, with the declared stacks and
// sample counts.
func TestProfilesMatchDeclaration(t *testing.T) {
	f := Default("r1", testStart)
	profiles := f.Profiles()
	pods := 0
	for _, svc := range f.Services {
		pods += len(svc.Pods)
	}
	if len(profiles) != pods*ProfileUploads {
		t.Fatalf("%d profiles, want %d", len(profiles), pods*ProfileUploads)
	}
	for i, p := range profiles {
		if want := testStart.Add(time.Duration(i%ProfileUploads) * ProfileInterval); !p.Start.Equal(want) {
			t.Errorf("profile %d starts at %v, want %v", i, p.Start, want)
		}
		query, contentType, body := f.PyroscopeIngest(p)
		if want := p.Service + "{" + RunLabel + "=r1,pod=" + p.Pod + "}"; query.Get("name") != want {
			t.Errorf("profile %d: name %q, want %q", i, query.Get("name"), want)
		}
		_, params, err := mime.ParseMediaType(contentType)
		if err != nil {
			t.Fatal(err)
		}
		form, err := multipart.NewReader(bytes.NewReader(body), params["boundary"]).ReadForm(1 << 20)
		if err != nil {
			t.Fatal(err)
		}
		file, err := form.File["profile"][0].Open()
		if err != nil {
			t.Fatal(err)
		}
		prof, err := profile.Parse(file)
		file.Close()
		if err != nil {
			t.Fatalf("profile %d: %v", i, err)
		}
		if len(prof.Sample) != len(p.Stacks) {
			t.Fatalf("profile %d: %d samples, want %d", i, len(prof.Sample), len(p.Stacks))
		}
		for n, sample := range prof.Sample {
			var frames []string
			for _, loc := range sample.Location {
				frames = append(frames, loc.Line[0].Function.Name)
			}
			if !slices.Equal(frames, p.Stacks[n]) {
				t.Errorf("profile %d sample %d: stack %v, want %v", i, n, frames, p.Stacks[n])
			}
			if want := []int64{p.Samples[n], p.Samples[n] * int64(CPUPeriod)}; !slices.Equal(sample.Value, want) {
				t.Errorf("profile %d sample %d: values %v, want %v", i, n, sample.Value, want)
			}
		}
	}
}
