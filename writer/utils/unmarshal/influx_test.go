package unmarshal

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/go-faster/jx"
	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/utils"
)

const LEN = 64

func TestDDTags(t *testing.T) {
	var tagPattern = regexp.MustCompile(`([\p{L}][\p{L}_0-9\-.\\/]*):([\p{L}_0-9\-.\\/:]+)(,|$)`)
	for _, match := range tagPattern.FindAllStringSubmatch("env:staging,version:5.1,", -1) {
		println(match[1], match[2])
	}
}

func TestAppend(t *testing.T) {
	a := make([]string, 0, 10)
	b := append(a, "a")
	fmt.Println(b[0])
	a = a[:1]
	fmt.Println(a[0])
}

func BenchmarkFastAppend(b *testing.B) {
	for b.Loop() {
		var res []byte
		res = append(res, fastFillArray(LEN, byte(1))...)
		_ = res
	}
}

func BenchmarkAppend(b *testing.B) {
	for b.Loop() {
		var res []byte
		for range LEN {
			res = append(res, 1)
		}
		_ = res
	}
}

func BenchmarkAppendFill(b *testing.B) {
	a := make([]byte, 0, LEN)
	for b.Loop() {
		for range LEN {
			a = append(a, 5)
		}
		_ = a
	}
}

func TestJsonError(t *testing.T) {
	r := jx.Decode(strings.NewReader(`123`), 1024)
	fmt.Println(r.BigInt())
	//fmt.Println(r.Str())
}

type influxEntry struct {
	labels    map[string]string
	timestamp int64
	message   string
	value     float64
	tp        uint8
}

func decodeInflux(t *testing.T, body string, precision lineprotocol.Precision) []influxEntry {
	t.Helper()
	dec := &influxDec{ctx: &ParserCtx{
		bodyReader: strings.NewReader(body),
		ctx:        context.WithValue(context.Background(), utils.ContextKeyPrecision, precision),
	}}
	var res []influxEntry
	dec.SetOnEntries(func(labels [][]string, timestampsNS []int64, message []string,
		value []float64, types []uint8) error {
		lbls := map[string]string{}
		for _, l := range labels {
			lbls[l[0]] = l[1]
		}
		res = append(res, influxEntry{lbls, timestampsNS[0], message[0], value[0], types[0]})
		return nil
	})
	if err := dec.Decode(); err != nil {
		t.Fatalf("decode %q: %v", body, err)
	}
	return res
}

func TestInfluxMetrics(t *testing.T) {
	entries := decodeInflux(t, "cpu,host=a,region=eu-1 usage.idle=99.5,count=3i,ignored=\"x\" 1600000000\n",
		lineprotocol.Millisecond)
	if len(entries) != 2 {
		t.Fatalf("want 2 entries, got %d: %+v", len(entries), entries)
	}
	byName := map[string]influxEntry{}
	for _, e := range entries {
		if e.tp != model.SAMPLE_TYPE_METRIC {
			t.Fatalf("want metric sample, got %d", e.tp)
		}
		if e.timestamp != 1600000000*int64(time.Millisecond) {
			t.Fatalf("want ms precision timestamp, got %d", e.timestamp)
		}
		if e.labels["measurement"] != "cpu" || e.labels["host"] != "a" || e.labels["region"] != "eu-1" {
			t.Fatalf("unexpected labels: %v", e.labels)
		}
		byName[e.labels["__name__"]] = e
	}
	// string fields are skipped, dots in field names are sanitized
	if e, ok := byName["usage_idle"]; !ok || e.value != 99.5 {
		t.Fatalf("usage_idle: %+v", byName)
	}
	if e, ok := byName["count"]; !ok || e.value != 3 {
		t.Fatalf("count: %+v", byName)
	}
}

func TestInfluxLogs(t *testing.T) {
	entries := decodeInflux(t, "syslog,host=a message=\"hello\",severity=\"warn\" 1600000000000000000\n",
		lineprotocol.Nanosecond)
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	e := entries[0]
	if e.tp != model.SAMPLE_TYPE_LOG || e.timestamp != 1600000000000000000 {
		t.Fatalf("unexpected entry: %+v", e)
	}
	if !strings.Contains(e.message, "message=hello") || !strings.Contains(e.message, "severity=warn") {
		t.Fatalf("unexpected message: %q", e.message)
	}
}

func TestInfluxNoTimestamp(t *testing.T) {
	before := time.Now().Truncate(time.Second).UnixNano()
	entries := decodeInflux(t, "cpu value=1\n", lineprotocol.Second)
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	if entries[0].timestamp < before || entries[0].timestamp%int64(time.Second) != 0 {
		t.Fatalf("want now truncated to seconds, got %d", entries[0].timestamp)
	}
}

func TestInfluxParseError(t *testing.T) {
	dec := &influxDec{ctx: &ParserCtx{
		bodyReader: strings.NewReader("cpu,host=a\n"),
		ctx:        context.WithValue(context.Background(), utils.ContextKeyPrecision, lineprotocol.Nanosecond),
	}}
	dec.SetOnEntries(func([][]string, []int64, []string, []float64, []uint8) error { return nil })
	if err := dec.Decode(); err == nil {
		t.Fatal("want an error for a line without fields")
	}
}
