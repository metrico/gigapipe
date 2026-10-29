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
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"
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

// Pod is one log stream of a service: Lines lines, ErrorLines of them at
// level "error" and the rest at level "info".
type Pod struct {
	Name       string
	Lines      int
	ErrorLines int
}

// Default returns the standard fixture used by the integration tests.
func Default(run string, start time.Time) Fixture {
	return Fixture{
		Run:   run,
		Start: start,
		Services: []Service{
			{Name: "checkout", Pods: []Pod{
				{Name: "p1", Lines: 4, ErrorLines: 1},
				{Name: "p2", Lines: 7, ErrorLines: 3},
				{Name: "p3", Lines: 1, ErrorLines: 0},
			}},
			{Name: "payments", Pods: []Pod{
				{Name: "p1", Lines: 5, ErrorLines: 2},
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
