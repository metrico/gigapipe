package insert

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/service"
)

func metricSvc(t *testing.T, newSvc func(model.InsertServiceOpts) service.IInsertServiceV2, cluster string) *service.InsertServiceV2Multimodal {
	t.Helper()
	service.CreateColPools(0)
	node := &model.DataDatabasesMap{ClokiBaseDataBase: config.ClokiBaseDataBase{ClusterName: cluster}}
	svc, ok := newSvc(model.InsertServiceOpts{Node: node}).(*service.InsertServiceV2Multimodal)
	if !ok {
		t.Fatal("metric insert services run on InsertServiceV2Multimodal")
	}
	return svc
}

// columns runs the service's request processing over req and returns the
// resulting insert columns by name, in order.
func columns(t *testing.T, svc *service.InsertServiceV2Multimodal, req any) ([]string, map[string]proto.ColInput) {
	t.Helper()
	n, cols, err := svc.ProcessRequest(req, svc.AcquireColumns())
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	byName := map[string]proto.ColInput{}
	for _, c := range cols {
		in := c.Input()
		names = append(names, in.Name)
		byName[in.Name] = in.Data
		if in.Data.Rows() != n {
			t.Fatalf("column %s has %d rows, the service reported %d", in.Name, in.Data.Rows(), n)
		}
	}
	return names, byName
}

func ms(v int64) time.Time { return time.UnixMilli(v).UTC() }

func TestMetricStagingRows(t *testing.T) {
	svc := metricSvc(t, NewMetricStagingInsertService, "")
	if want := "INSERT INTO metric_samples_in (fingerprint, timestamp, value, prev_timestamp, prev_value, aggregate)"; svc.InsertRequest != want {
		t.Fatalf("insert request: got %q, want %q", svc.InsertRequest, want)
	}
	stale := math.Float64frombits(0x7ff0000000000002)
	names, cols := columns(t, svc, &model.MetricSamplesData{
		MFingerprint:     []uint64{7, 7},
		MTimestampMs:     []int64{1000, 2000},
		MValue:           []float64{1.5, stale},
		MPrevTimestampMs: []int64{0, 1000},
		MPrevValue:       []float64{0, 1.5},
		MAggregate:       []uint8{1, 1},
	})
	if want := []string{"fingerprint", "timestamp", "value", "prev_timestamp", "prev_value", "aggregate"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("columns: got %v, want %v", names, want)
	}
	if got := cols["timestamp"].Type(); got != "DateTime64(3)" {
		t.Fatalf("timestamp type: got %s", got)
	}
	ts := cols["timestamp"].(*proto.ColDateTime64)
	prevTs := cols["prev_timestamp"].(*proto.ColDateTime64)
	if !ts.Row(1).Equal(ms(2000)) || !prevTs.Row(0).Equal(ms(0)) || !prevTs.Row(1).Equal(ms(1000)) {
		t.Fatalf("timestamps: got %v %v / %v %v", ts.Row(0), ts.Row(1), prevTs.Row(0), prevTs.Row(1))
	}
	values := cols["value"].(proto.ColFloat64)
	if values[0] != 1.5 || math.Float64bits(values[1]) != 0x7ff0000000000002 {
		t.Fatalf("values must be passed bit-exact, got %#x", math.Float64bits(values[1]))
	}
	if fp := cols["fingerprint"].(proto.ColUInt64); fp[0] != 7 || fp[1] != 7 {
		t.Fatalf("fingerprints: got %v", fp)
	}
	if prev := cols["prev_value"].(proto.ColFloat64); prev[1] != 1.5 {
		t.Fatalf("prev_value: got %v", prev)
	}
	if agg := cols["aggregate"].(proto.ColUInt8); agg[0] != 1 || agg[1] != 1 {
		t.Fatalf("aggregate: got %v", agg)
	}
}

func TestMetricSeriesRows(t *testing.T) {
	svc := metricSvc(t, NewMetricSeriesInsertService, "")
	if want := "INSERT INTO metric_series (name, fingerprint, labels, first_seen, last_seen)"; svc.InsertRequest != want {
		t.Fatalf("insert request: got %q, want %q", svc.InsertRequest, want)
	}
	labels := map[string]string{"__name__": "up", "job": "api", "service_name": "api"}
	names, cols := columns(t, svc, &model.MetricSeriesData{
		MName:        []string{"up"},
		MFingerprint: []uint64{7},
		MLabels:      []map[string]string{labels},
		MFirstSeenMs: []int64{1000},
		MLastSeenMs:  []int64{2000},
	})
	if want := []string{"name", "fingerprint", "labels", "first_seen", "last_seen"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("columns: got %v, want %v", names, want)
	}
	if got := cols["labels"].Type(); got != "Map(LowCardinality(String), String)" {
		t.Fatalf("labels type: got %s", got)
	}
	if got := cols["labels"].(*proto.ColMap[string, string]).Row(0); !reflect.DeepEqual(got, labels) {
		t.Fatalf("labels: got %v, want %v", got, labels)
	}
	if got := cols["name"].(*proto.ColLowCardinality[string]).Row(0); got != "up" {
		t.Fatalf("name: got %q", got)
	}
	for col, want := range map[string]proto.ColumnType{
		"first_seen": "SimpleAggregateFunction(min, DateTime64(3))",
		"last_seen":  "SimpleAggregateFunction(max, DateTime64(3))",
	} {
		if got := cols[col].Type(); got != want {
			t.Fatalf("%s type: got %s, want %s", col, got, want)
		}
		if err := cols[col].(proto.Inferable).Infer(want); err != nil {
			t.Fatalf("%s must accept the table's column type: %v", col, err)
		}
	}
	if !cols["first_seen"].(*service.ColSimpleAggDateTime64).Row(0).Equal(ms(1000)) ||
		!cols["last_seen"].(*service.ColSimpleAggDateTime64).Row(0).Equal(ms(2000)) {
		t.Fatal("first_seen/last_seen do not match the series' sample instants")
	}
}

func TestMetricMetadataRows(t *testing.T) {
	svc := metricSvc(t, NewMetricMetadataInsertService, "")
	if want := "INSERT INTO metric_metadata (name, type, help, unit, updated_at)"; svc.InsertRequest != want {
		t.Fatalf("insert request: got %q, want %q", svc.InsertRequest, want)
	}
	names, cols := columns(t, svc, &model.MetricMetadataData{
		MName: []string{"up"}, MType: []string{"gauge"}, MHelp: []string{"Target is up."},
		MUnit: []string{""}, MUpdatedAtMs: []int64{5000},
	})
	if want := []string{"name", "type", "help", "unit", "updated_at"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("columns: got %v, want %v", names, want)
	}
	if got := cols["type"].(*proto.ColLowCardinality[string]).Row(0); got != "gauge" {
		t.Fatalf("type: got %q", got)
	}
	if got := cols["help"].(*proto.ColStr).Row(0); got != "Target is up." {
		t.Fatalf("help: got %q", got)
	}
	if !cols["updated_at"].(*proto.ColDateTime64).Row(0).Equal(ms(5000)) {
		t.Fatal("updated_at mismatch")
	}
}

func TestMetricExemplarRows(t *testing.T) {
	svc := metricSvc(t, NewMetricExemplarsInsertService, "")
	if want := "INSERT INTO metric_exemplars (fingerprint, timestamp, value, trace_id, labels)"; svc.InsertRequest != want {
		t.Fatalf("insert request: got %q, want %q", svc.InsertRequest, want)
	}
	names, cols := columns(t, svc, &model.MetricExemplarsData{
		MFingerprint: []uint64{7}, MTimestampMs: []int64{990}, MValue: []float64{0.5},
		MTraceID: []string{"abc"}, MLabels: []string{`{"trace_id":"abc"}`},
	})
	if want := []string{"fingerprint", "timestamp", "value", "trace_id", "labels"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("columns: got %v, want %v", names, want)
	}
	if got := cols["labels"].(*proto.ColStr).Row(0); got != `{"trace_id":"abc"}` {
		t.Fatalf("labels: got %q", got)
	}
	if got := cols["trace_id"].(*proto.ColStr).Row(0); got != "abc" {
		t.Fatalf("trace_id: got %q", got)
	}
}

func TestMetricServicesOnClusterUseDist(t *testing.T) {
	for table, newSvc := range map[string]func(model.InsertServiceOpts) service.IInsertServiceV2{
		"metric_samples_in": NewMetricStagingInsertService,
		"metric_series":     NewMetricSeriesInsertService,
		"metric_metadata":   NewMetricMetadataInsertService,
		"metric_exemplars":  NewMetricExemplarsInsertService,
	} {
		svc := metricSvc(t, newSvc, "c1")
		if !strings.HasPrefix(svc.InsertRequest, "INSERT INTO "+table+"_dist ") {
			t.Errorf("%s on a cluster: got %q", table, svc.InsertRequest)
		}
	}
}
