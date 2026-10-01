package unmarshal

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/go-faster/jx"
	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/utils"
	"github.com/metrico/qryn/v5/writer/utils/metriccache"
	"github.com/metrico/qryn/v5/writer/utils/numbercache"
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
		res = append(res, slices.Repeat([]byte{1}, LEN)...)
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
	nLabels   int
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
		res = append(res, influxEntry{lbls, len(labels), timestampsNS[0], message[0], value[0], types[0]})
		return nil
	})
	if err := dec.Decode(); err != nil {
		t.Fatalf("decode %q: %v", body, err)
	}
	return res
}

type influxLogRow struct {
	tsNs    int64
	message string
	tp      uint8
}

// pushInflux runs a line-protocol body through the Influx parser and collects
// the metric rows and the log rows it produces.
func pushInflux(t *testing.T, body string, precision lineprotocol.Precision) (metricRows, []influxLogRow) {
	t.Helper()
	withCityHashFingerprints(t)
	ctx := context.WithValue(context.Background(), utils.ContextKeyPrecision, precision)
	ctx = metriccache.NewContext(ctx, newNode(t))
	var logRows []influxLogRow
	metrics := make(chan *model.ParserResponse)
	go func() {
		defer close(metrics)
		for resp := range UnmarshalInfluxDBLogsV2(ctx, strings.NewReader(body), newTestFPCache(t)) {
			if d, ok := resp.SamplesRequest.(*model.TimeSamplesData); ok {
				for i := range d.MMessage {
					logRows = append(logRows, influxLogRow{d.MTimestampNS[i], d.MMessage[i], d.MType[i]})
				}
			}
			resp.SamplesRequest, resp.TimeSeriesRequest = nil, nil
			metrics <- resp
		}
	}()
	return collectMetricRows(t, metrics), logRows
}

func seriesByName(rows metricRows) map[string]seriesRow {
	res := map[string]seriesRow{}
	for _, s := range rows.series {
		res[s.name] = s
	}
	return res
}

func TestInfluxLineWithoutMessageIsOneSamplePerNumericField(t *testing.T) {
	rows, logs := pushInflux(t, "cpu,host=a,region=eu-1 usage.idle=99.5,count=3i,ignored=\"x\" 1600000000123456789\n",
		lineprotocol.Nanosecond)
	if len(logs) != 0 {
		t.Fatalf("log rows: got %+v, want none", logs)
	}
	series := seriesByName(rows)
	if len(series) != 2 {
		t.Fatalf("series rows: got %+v, want usage_idle and count", rows.series)
	}
	values := map[string]float64{"usage_idle": 99.5, "count": 3}
	for name, want := range values {
		s, ok := series[name]
		if !ok {
			t.Fatalf("no series %s in %+v", name, rows.series)
		}
		wantLabels := map[string]string{"__name__": name, "measurement": "cpu", "host": "a", "region": "eu-1",
			"service_name": "unknown"}
		if len(s.labels) != len(wantLabels) {
			t.Fatalf("series %s labels: got %v, want %v", name, s.labels, wantLabels)
		}
		for k, v := range wantLabels {
			if s.labels[k] != v {
				t.Fatalf("series %s label %s: got %q, want %q", name, k, s.labels[k], v)
			}
		}
		var found bool
		for _, r := range rows.staging {
			if r.fp == s.fp {
				found = true
				if r.tsMs != 1600000000123 || r.value != want {
					t.Errorf("series %s sample: got %+v, want ts 1600000000123 value %v", name, r, want)
				}
			}
		}
		if !found {
			t.Errorf("no staging row for %s", name)
		}
	}
}

func TestInfluxLineWithMessageStaysALog(t *testing.T) {
	rows, logs := pushInflux(t, "syslog,host=a message=\"hello\",severity=\"warn\" 1600000000000000000\n"+
		"cpu value=1 1600000000000000000\n", lineprotocol.Nanosecond)
	if len(logs) != 1 {
		t.Fatalf("log rows: got %+v, want 1", logs)
	}
	if l := logs[0]; l.tp != model.SAMPLE_TYPE_LOG || l.tsNs != 1600000000000000000 || !strings.Contains(l.message, "message=hello") {
		t.Fatalf("log row: got %+v", l)
	}
	if len(rows.series) != 1 || rows.series[0].name != "value" || len(rows.staging) != 1 {
		t.Fatalf("metric rows: got %+v, want one sample of value", rows)
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
	before := time.Now().Truncate(time.Second).UnixMilli()
	rows, _ := pushInflux(t, "cpu value=1\n", lineprotocol.Second)
	if len(rows.staging) != 1 {
		t.Fatalf("want 1 sample, got %+v", rows.staging)
	}
	if ts := rows.staging[0].tsMs; ts < before || ts%1000 != 0 {
		t.Fatalf("want now truncated to seconds, got %d", ts)
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

func TestInfluxDuplicateTag(t *testing.T) {
	rows, _ := pushInflux(t, "cpu,host=a,host=b value=1 1\n", lineprotocol.Nanosecond)
	if len(rows.series) != 1 {
		t.Fatalf("want 1 series, got %+v", rows.series)
	}
	lbls := rows.series[0].labels
	if lbls["host"] != "b" {
		t.Fatalf("want the last tag value to win, got %q", lbls["host"])
	}
	if len(lbls) != 4 { // measurement, host, __name__, service_name
		t.Fatalf("want deduplicated labels, got %v", lbls)
	}
}

func newTestFPCache(t *testing.T) numbercache.ICache[uint64] {
	t.Helper()
	cache := numbercache.NewCache(time.Minute, func(val uint64) []byte {
		return unsafe.Slice((*byte)(unsafe.Pointer(&val)), 8)
	}, map[string]*model.DataDatabasesMap{})
	t.Cleanup(cache.Stop)
	return cache
}
