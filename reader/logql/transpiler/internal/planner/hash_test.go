package planner

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/go-faster/city"
	"github.com/metrico/qryn/reader/logql/transpiler/shared"
)

func BenchmarkCH(b *testing.B) {
	labels := map[string]string{
		"a": "b",
		"b": "b",
		"c": "b",
		"d": "b",
	}
	for i := 0; i < b.N; i++ {
		city.CH64([]byte(fmt.Sprintf("%v", labels)))
	}
}

func BenchmarkCH2(b *testing.B) {
	labels := map[string]string{
		"a": "b",
		"b": "b",
		"c": "b",
		"d": "b",
	}
	for i := 0; i < b.N; i++ {
		descr := [3]uint64{0, 0, 1}
		for k, v := range labels {
			a := k + v
			descr[0] += city.CH64([]byte(a))
			descr[1] ^= city.CH64([]byte(a))
			descr[2] *= 1779033703 + 2*city.CH64([]byte(a))

		}
		city.CH64(unsafe.Slice((*byte)(unsafe.Pointer(&descr[0])), 24))
	}
}

func BenchmarkDelete(b *testing.B) {
	for i := 0; i < b.N; i++ {
		labels := map[string]string{
			"a": "b",
			"b": "b",
			"c": "b",
			"d": "b",
		}
		for k := range labels {
			if k != "a" {
				delete(labels, k)
			}
		}
	}
}

func BenchmarkDelete2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		labels := map[string]string{
			"a": "b",
			"b": "b",
			"c": "b",
			"d": "b",
		}
		_labels := make(map[string]string)
		for k, v := range labels {
			if k == "a" {
				_labels[k] = v
			}
		}
	}
}

func BenchmarkMap(b *testing.B) {
	labels := map[string]string{
		"a": "b",
		"b": "",
	}
	for i := 0; i < b.N; i++ {
		_labels := map[string]string{
			"a":   "b",
			"b":   "b",
			"c":   "a",
			"d":   "q",
			"f":   "rt",
			"sda": "wrwer",
		}
		for k, v := range _labels {
			if labels[k] == v || labels[k] == "" {
				delete(_labels, k)
			}
		}
	}
}

func BenchmarkMap2(b *testing.B) {
	labels := []string{"a", "b"}
	values := []string{"b", ""}
	for i := 0; i < b.N; i++ {
		_labels := map[string]string{
			"a":   "b",
			"b":   "b",
			"c":   "a",
			"d":   "q",
			"f":   "rt",
			"sda": "wrwer",
		}
		for k, v := range _labels {
			for i, l := range labels {
				if k == l && (v == values[i] || values[i] == "") {
					delete(_labels, k)
				}
			}
		}
	}
}

type FakePlanner struct {
	out chan []shared.LogEntry
}

func (f FakePlanner) IsMatrix() bool {
	return false
}

func (f FakePlanner) Process(context *shared.PlannerContext, c chan []shared.LogEntry) (chan []shared.LogEntry, error) {
	return f.out, nil
}

func TestParser(t *testing.T) {
	out := make(chan []shared.LogEntry)
	p := ParserPlanner{
		GenericPlanner:  GenericPlanner{Main: &FakePlanner{out}},
		Op:              "logfmt",
		ParameterNames:  []string{"lbl"},
		ParameterValues: []string{"a"},
	}
	in, err := p.Process(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		out <- []shared.LogEntry{{
			TimestampNS: 1,
			Fingerprint: 1,
			Labels:      map[string]string{"a": "b", "b": "b"},
			Message:     `a=dgdfgdfgdgf`,
			Value:       0,
			Err:         nil,
		}}
	}()
	fmt.Println(<-in)
}
