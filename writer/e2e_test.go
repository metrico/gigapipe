package writer

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/qryn/reader/utils/middleware"
	"github.com/metrico/qryn/writer/chwrapper"
	"github.com/metrico/qryn/writer/plugin"
	"github.com/metrico/qryn/writer/utils/logger"
	"github.com/mochi-co/mqtt/server/listeners"
	"github.com/openzipkin/zipkin-go/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/golang/snappy"
	json "github.com/json-iterator/go"
	"github.com/m3db/prometheus_remote_client_golang/promremote"
	"github.com/metrico/qryn/writer/config"
	"github.com/metrico/qryn/writer/utils/proto/logproto"
	mqtt "github.com/mochi-co/mqtt/server"
	zipkin "github.com/openzipkin/zipkin-go/reporter/http"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
	"google.golang.org/protobuf/proto"
)

/*
stream / values format

{
  "streams": [
    {
      "stream": {
        "label": "value"
      },
      "values": [
          [ "<unix epoch in nanoseconds>", "<log line>" ],
          [ "<unix epoch in nanoseconds>", "<log line>" ]
      ]
    }
  ]
}

*/

var serviceInfo plugin.ServicesObject

func genLines(_labels map[string]string, freqS float64, testid string, fromNS int64, toNS int64,
	lineGen func(int) string, valGen func(int) float64) map[string]interface{} {
	if lineGen == nil {
		lineGen = func(i int) string {
			return fmt.Sprintf("TEST_LINE_%d", i)
		}
	}
	_labels["test_id"] = testid
	freqNS := int64(float64(time.Second.Nanoseconds()) * freqS)
	valLen := int64((toNS - fromNS) / freqNS)
	values := make([][]interface{}, valLen)
	for i := int64(0); i < valLen; i++ {
		values[i] = []interface{}{
			fmt.Sprintf("%d", fromNS+(i*freqNS)),
			lineGen(int(i)),
		}
		if valGen != nil {
			values[i] = append(values[i], valGen(int(i)))
		}
	}
	fmt.Printf("Created %d VALUES\n", len(values))
	return map[string]interface{}{
		"stream": _labels,
		"values": values,
	}
}

/*
labels / entries format


  "streams": [
    {
      "labels": "<LogQL label key-value pairs>",
      "entries": [
        {
          "ts": "<RFC3339Nano timestamp>",
          "line": "<log line>"
        }
      ]
    }
  ]
}

*/

func genOldLines(_labels map[string]string, freqS float64, testid string, fromNS int64, toNS int64,
	lineGen func(int) string, valGen func(int) float64) map[string]interface{} {
	if lineGen == nil {
		lineGen = func(i int) string {
			return fmt.Sprintf("TEST_LINE_%d", i)
		}
	}
	strLabels := make([]string, len(_labels), len(_labels)+2)
	i := int64(0)
	for k, v := range _labels {
		strLabels[i] = k + "=\"" + v + "\""
	}
	strLabels = append(strLabels, fmt.Sprintf("test_id=\"%s\"", testid))
	freqNS := int64(float64(time.Second.Nanoseconds()) * freqS)
	entLen := int64((toNS - fromNS) / freqNS)
	entries := make([]map[string]interface{}, entLen)
	size := 0
	for i = 0; i < entLen; i++ {
		entries[i] = map[string]interface{}{
			"ts":   fmt.Sprintf("%d", fromNS+(i*freqNS)),
			"line": lineGen(int(i)),
		}
		if valGen != nil {
			entries[i]["value"] = valGen(int(i))
		}
		size += len(entries[i]["ts"].(string)) + len(entries[i]["line"].(string))
	}
	fmt.Printf("Created %d VALUES\n", i)
	return map[string]interface{}{
		"labels":  "{" + strings.Join(strLabels, ",") + "}",
		"entries": entries,
	}
}

func genProtoLines(_labels map[string]string, freqS float64, testid string, fromNS int64, toNS int64,
	lineGen func(int) string) []byte {
	if lineGen == nil {
		lineGen = func(i int) string {
			return fmt.Sprintf("TEST_LINE_%d", i)
		}
	}
	_labels["test_id"] = testid
	strLabels := make([]string, len(_labels))
	i := 0
	for k, v := range _labels {
		strLabels[i] = fmt.Sprintf(`%s="%s"`, k, v)
		i++
	}
	req := logproto.PushRequest{Streams: make([]*logproto.StreamAdapter, 1)}
	req.Streams[0] = &logproto.StreamAdapter{
		Labels:  "{" + strings.Join(strLabels, ",") + "}",
		Entries: make([]*logproto.EntryAdapter, 0, 1000),
	}
	for i := fromNS; i < toNS; i += int64(freqS * 1e9) {
		req.Streams[0].Entries = append(req.Streams[0].Entries, &logproto.EntryAdapter{
			Line: lineGen(int(i)),
			Timestamp: &logproto.Timestamp{
				Seconds: i / 1e9,
				Nanos:   int32(i % 1e9),
			},
		})
	}
	fmt.Printf("Created %d VALUES\n", len(req.Streams[0].Entries))
	byteReq, err := proto.Marshal(&req)
	if err != nil {
		panic(err)
	}
	var compReq []byte = nil
	compReq = snappy.Encode(compReq, byteReq)
	return compReq
}

func request(body []byte, contentType string) error {
	fmt.Println("Requesting")
	req := fasthttp.AcquireRequest()
	req.SetBody(body)
	req.Header.Set("Content-Type", contentType /*"application/json"*/)
	req.Header.Set("X-Scope-OrgID", "1")
	req.Header.Set("X-Logs-Daily-MB", "1000")
	req.SetRequestURI("http://localhost:3215/loki/api/v1/push")
	req.Header.SetMethod("POST")
	resp := fasthttp.AcquireResponse()
	err := fasthttp.Do(req, resp)
	defer fasthttp.ReleaseResponse(resp)
	defer fasthttp.ReleaseRequest(req)
	if err != nil {
		fmt.Println("Requesting ERR")
		return err
	}
	if resp.StatusCode() != 204 {
		fmt.Println("Requesting ERR #2")
		return fmt.Errorf("[%d] %s", resp.StatusCode(), resp.Body())
	}
	fmt.Println("Requesting OK")
	return nil
}

func getSamplesTable() string {
	return config.Cloki.Setting.DATABASE_DATA[0].TableSamples
}

func getTSTable() string {
	return config.Cloki.Setting.DATABASE_DATA[0].TableSeries
}

func getTestIDData(testID string) []string {
	var fp uint64
	//client, err := adapter.NewClient(context.Background(), &config.Cloki.Setting.DATABASE_DATA[0], true)
	client, err := chwrapper.NewSmartDatabaseAdapter(&config.Cloki.Setting.DATABASE_DATA[0], true)
	if err != nil {
		panic(err)
	}
	err = client.GetFirst(fmt.Sprintf("select distinct fingerprint from "+getTSTable()+" where "+
		"JSONExtractString(labels, 'test_id') == '%s' AND org_id == '1'", testID), &fp)
	logger.Info("select distinct fingerprint from "+getTSTable()+" where "+
		"JSONExtractString(labels, 'test_id') == '%s' AND org_id == '1'", testID)
	if err != nil {
		fmt.Println("Error 1...", err.Error())
		panic(err)
	}

	arr, err := client.GetList(fmt.Sprintf("select formatRow('TSV', timestamp_ns, "+
		"arraySort(JSONExtractKeysAndValues(labels, 'String')), string, value) "+
		"from "+getSamplesTable()+" as samples_v4 "+
		"left any join "+config.Cloki.Setting.DATABASE_DATA[0].Name+".time_series_v2 "+
		"     ON samples_v4.fingerprint == time_series_v2.fingerprint "+
		" where fingerprint = %d AND org_id == '1' ORDER BY timestamp_ns ASC", fp))
	if err != nil {
		fmt.Println("Error 2..... ", err.Error())
		panic(err)
	}
	return arr
}

func testPrometheusPush(t *testing.T, testid int64) {
	cfg := promremote.NewConfig(
		promremote.WriteURLOption("http://localhost:3215/prom/remote/write"),
		promremote.HTTPClientTimeoutOption(60*time.Second),
		promremote.UserAgent("go-test"),
	)

	client, err := promremote.NewClient(cfg)
	if err != nil {
		t.Fatal(fmt.Errorf("unable to construct client: %v", err))
	}
	now := time.Now()
	timeSeriesList := []promremote.TimeSeries{
		{
			Labels: []promremote.Label{
				{
					Name:  "test_id",
					Value: fmt.Sprintf("foo_bar_%d", testid),
				},
				{
					Name:  "biz",
					Value: "baz",
				},
			},
			Datapoint: promremote.Datapoint{
				Timestamp: now,
				Value:     1415.92,
			},
		},
	}
	if _, err = client.WriteTimeSeries(context.Background(), timeSeriesList, promremote.WriteOptions{
		Headers: map[string]string{"X-Scope-OrgID": "1"},
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 2)
	values := getTestIDData(fmt.Sprintf("foo_bar_%d", testid))
	assert.Equal(t, []string{fmt.Sprintf(
		"%d\t[('biz','baz'),('test_id','foo_bar_%d')]\t\t1415.92\n",
		now.UnixMilli()*1000000,
		testid,
	)}, values)
}

func mustMarshal(i interface{}) []byte {
	res, err := json.Marshal(i)
	if err != nil {
		panic(err)
	}
	return res
}

var appFlags CommandLineFlags

// params for Flags
type CommandLineFlags struct {
	InitializeDB    *bool   `json:"initialize_db"`
	ShowHelpMessage *bool   `json:"help"`
	ShowVersion     *bool   `json:"version"`
	ConfigPath      *string `json:"config_path"`
}

/* init flags */
func initFlags() {
	appFlags.InitializeDB = flag.Bool("initialize_db", false, "initialize the database and create all tables")
	appFlags.ShowHelpMessage = flag.Bool("help", false, "show help")
	appFlags.ShowVersion = flag.Bool("version", false, "show version")
	appFlags.ConfigPath = flag.String("config", "", "the path to the config file")
	flag.Parse()
}

func TestE2E(t *testing.T) {
	if os.Getenv("E2E") != "1" {
		return
	}
	os.Args = append(os.Args, "-config", os.Getenv("CONFIG"))
	rand.Seed(time.Now().UnixNano())
	testId := rand.Int63()
	go runPrometheus(testId)
	go runMQTT()

	initFlags()

	/* first check admin flags */
	//	checkHelpVersionFlags()
	var configPaths []string
	if _, err := os.Stat(*appFlags.ConfigPath); err == nil {
		configPaths = append(configPaths, *appFlags.ConfigPath)
	}
	config.Cloki = clconfig.New(clconfig.CLOKI_WRITER, configPaths, "", "")

	////ReadConfig
	config.Cloki.ReadConfig()
	//checkLicenseFlags()

	app := mux.NewRouter()
	if config.Cloki.Setting.AUTH_SETTINGS.BASIC.Username != "" &&
		config.Cloki.Setting.AUTH_SETTINGS.BASIC.Password != "" {
		app.Use(middleware.BasicAuthMiddleware(config.Cloki.Setting.AUTH_SETTINGS.BASIC.Username,
			config.Cloki.Setting.AUTH_SETTINGS.BASIC.Password))
	}
	app.Use(middleware.AcceptEncodingMiddleware)
	if config.Cloki.Setting.HTTP_SETTINGS.Cors.Enable {
		app.Use(middleware.CorsMiddleware(config.Cloki.Setting.HTTP_SETTINGS.Cors.Origin))
	}
	app.Use(middleware.LoggingMiddleware("[{{.status}}] {{.method}} {{.url}} - LAT:{{.latency}}"))

	Init(config.Cloki, app)
	var pluginInfo = &plugin.QrynWriterPlugin{}
	//pluginInfo.Initialize(appFlags)
	//////License After Config - need check some params
	//
	//pluginInfo.RegisterRoutes(*config.Cloki.Setting)

	serviceInfo = pluginInfo.ServicesObject
	//go main()
	time.Sleep(5 * time.Second)

	end := time.Now().UnixNano() / 1e6 * 1e6
	start := end - time.Second.Nanoseconds()*2
	mid := start + time.Second.Nanoseconds()
	ln := func(i int) string {
		return "LINE"
	}
	val := func(i int) float64 {
		return 123
	}
	lines1 := map[string]interface{}{
		"streams": []map[string]interface{}{
			genLines(map[string]string{"test1": "val1"}, 1, fmt.Sprintf("TEST_%d", testId),
				start, mid, ln, val),
		},
	}
	lines2 := map[string]interface{}{
		"streams": []map[string]interface{}{
			genLines(map[string]string{"test1": "val1"}, 1, fmt.Sprintf("TEST_%d", testId),
				mid, end, ln, val),
		},
	}
	err := request(mustMarshal(lines1), "application/json")
	if err != nil {
		t.Fatal(err)
	}
	err = request(mustMarshal(lines2), "application/json")
	if err != nil {
		t.Fatal(err)
	}

	oldLines := map[string]interface{}{
		"streams": []map[string]interface{}{
			genOldLines(map[string]string{"test1": "val1"}, 1, fmt.Sprintf("OLD_TEST_%d", testId),
				start, end, ln, val),
		},
	}
	err = request(mustMarshal(oldLines), "application/json")
	if err != nil {
		t.Fatal(err)
	}
	err = request(
		genProtoLines(
			map[string]string{"test1": "val1"}, 1,
			fmt.Sprintf("PROTO_TEST_%d", testId),
			start, end, ln),
		"application/x-protobuf")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	fmt.Printf("TEST ID: %d\n", testId)
	getTestRes := func(id string, val float64) []string {
		return []string{
			fmt.Sprintf("%d	[('test1','val1'),('test_id','%s')]	LINE	%d\n", start, id, int(val)),
			fmt.Sprintf("%d	[('test1','val1'),('test_id','%s')]	LINE	%d\n", mid, id, int(val)),
		}
	}
	time.Sleep(2 * time.Second)
	values := getTestIDData(fmt.Sprintf("TEST_%d", testId))
	assert.Equal(t, getTestRes(fmt.Sprintf("TEST_%d", testId), 123), values)
	values = getTestIDData(fmt.Sprintf("OLD_TEST_%d", testId))
	assert.Equal(t, getTestRes(fmt.Sprintf("OLD_TEST_%d", testId), 123), values)
	values = getTestIDData(fmt.Sprintf("PROTO_TEST_%d", testId))
	assert.Equal(t, getTestRes(fmt.Sprintf("PROTO_TEST_%d", testId), 0), values)
	testPrometheusPush(t, testId)
	testPrometheusScrape(t, testId)
	testTempoZipkinIngest(t, uint64(testId))
	miscTest()
	ingestInfluxTest(t, uint64(testId))
	time.Sleep(5 * time.Second)
	testUsageCounting(t)
	//ingestInfluxJSONTest(t, uint64(testId))
}

var mqttServer *mqtt.Server

func runMQTT() {
	mqttServer = mqtt.New()
	tcp := listeners.NewTCP("t1", ":1883")
	err := mqttServer.AddListener(tcp, nil)
	if err != nil {
		panic(err)
	}
	err = mqttServer.Serve()
	if err != nil {
		panic(err)
	}
}

var promTestGauge prometheus.Gauge = nil
var promTestCounter prometheus.Counter = nil
var promTestHist prometheus.Histogram = nil
var promTestSumm prometheus.Summary = nil
var metricsSrv *http.Server

func runPrometheus(testID int64) error {
	cLbls := map[string]string{

		"test": fmt.Sprintf("promtest_%d", testID),
	}
	promTestGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace:   "test",
		Subsystem:   "test",
		Name:        fmt.Sprintf("testG_%d", testID),
		Help:        "test gauge",
		ConstLabels: cLbls,
	})
	promTestCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "test",
		Subsystem:   "test",
		Name:        fmt.Sprintf("testCnt_%d", testID),
		Help:        "test counter",
		ConstLabels: cLbls,
	})
	promTestHist = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace:   "test",
		Subsystem:   "test",
		Name:        fmt.Sprintf("testHist_%d", testID),
		Help:        "test hist",
		ConstLabels: cLbls,
		Buckets:     []float64{0, 10, 20, 30, 40, 50},
	})
	promTestSumm = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "test",
		Subsystem:   "test",
		Name:        fmt.Sprintf("testSumm_%d", testID),
		Help:        "test summ",
		ConstLabels: cLbls,
		Objectives:  nil,
		MaxAge:      time.Minute,
		AgeBuckets:  5,
		BufCap:      1000,
	})
	sm := http.NewServeMux()
	sm.Handle("/metrics", promhttp.Handler())
	metricsSrv = &http.Server{Addr: ":2112", Handler: sm}
	go metricsSrv.ListenAndServe()
	return nil
}

func testPrometheusScrape(t *testing.T, testID int64) {
	promTestGauge.Set(10)
	promTestCounter.Add(1)
	for i := 0; i <= 50; i = i + 10 {
		promTestHist.Observe(float64(i))
		promTestSumm.Observe(float64(i))
		promTestHist.Observe(float64(i))
		promTestSumm.Observe(float64(i))
	}
	time.Sleep(time.Second * 6)
	metricsSrv.Close()

	bytes := make([]byte, 0, 10000)
	code, bytes, _ := fasthttp.Get(bytes, "http://localhost:2112/metrics")
	fmt.Printf("[%d]: %s\n", code, bytes)
	//client, err := adapter.NewClient(context.Background(), &config.Cloki.Setting.DATABASE_DATA[0], true)
	client, err := chwrapper.NewSmartDatabaseAdapter(&config.Cloki.Setting.DATABASE_DATA[0], true)
	if err != nil {
		t.Fatal(err)
	}
	labels, err := client.GetList(fmt.Sprintf("SELECT DISTINCT labels "+
		"FROM "+getTSTable()+" WHERE JSONExtractString(labels, 'test') == 'promtest_%d' and org_id=='1' "+
		"ORDER BY labels", testID))

	if err != nil {
		t.Fatal(err)
	}

	labelsSet := make([]string, 0, 10)
	for _, label := range labels {
		label = strings.Replace(label, fmt.Sprintf("%d", testID), "", -1)
		labelsSet = append(labelsSet, label)
	}
	labelsMap := make([]map[string]string, 0, 20)
	for _, l := range labelsSet {
		_map := map[string]string{}
		json.UnmarshalFromString(l, &_map)
		labelsMap = append(labelsMap, _map)
	}
	sort.Slice(labelsMap, func(i, j int) bool {
		return fmt.Sprintf("%v", labelsMap[i]) < fmt.Sprintf("%v", labelsMap[j])
	})
	for _, l := range labelsMap {
		fmt.Printf("%v\n", l)
	}
	assert.Equal(t, []map[string]string{
		{"_SUBTYPE_": "bucket", "__name__": "test_test_testHist_", "__type__": "HISTOGRAM", "endpoint": "test_end", "instance": "test", "le": "+Inf", "test": "promtest_"},
		{"_SUBTYPE_": "bucket", "__name__": "test_test_testHist_", "__type__": "HISTOGRAM", "endpoint": "test_end", "instance": "test", "le": "0", "test": "promtest_"},
		{"_SUBTYPE_": "bucket", "__name__": "test_test_testHist_", "__type__": "HISTOGRAM", "endpoint": "test_end", "instance": "test", "le": "10", "test": "promtest_"},
		{"_SUBTYPE_": "bucket", "__name__": "test_test_testHist_", "__type__": "HISTOGRAM", "endpoint": "test_end", "instance": "test", "le": "20", "test": "promtest_"},
		{"_SUBTYPE_": "bucket", "__name__": "test_test_testHist_", "__type__": "HISTOGRAM", "endpoint": "test_end", "instance": "test", "le": "30", "test": "promtest_"},
		{"_SUBTYPE_": "bucket", "__name__": "test_test_testHist_", "__type__": "HISTOGRAM", "endpoint": "test_end", "instance": "test", "le": "40", "test": "promtest_"},
		{"_SUBTYPE_": "bucket", "__name__": "test_test_testHist_", "__type__": "HISTOGRAM", "endpoint": "test_end", "instance": "test", "le": "50", "test": "promtest_"},
		{"_SUBTYPE_": "count", "__name__": "test_test_testHist_", "__type__": "HISTOGRAM", "endpoint": "test_end", "instance": "test", "test": "promtest_"},
		{"_SUBTYPE_": "count", "__name__": "test_test_testSumm_", "__type__": "SUMMARY", "endpoint": "test_end", "instance": "test", "test": "promtest_"},
		{"_SUBTYPE_": "sum", "__name__": "test_test_testHist_", "__type__": "HISTOGRAM", "endpoint": "test_end", "instance": "test", "test": "promtest_"},
		{"_SUBTYPE_": "sum", "__name__": "test_test_testSumm_", "__type__": "SUMMARY", "endpoint": "test_end", "instance": "test", "test": "promtest_"},
		{"__name__": "test_test_testCnt_", "__type__": "COUNTER", "endpoint": "test_end", "instance": "test", "test": "promtest_"},
		{"__name__": "test_test_testG_", "__type__": "GAUGE", "endpoint": "test_end", "instance": "test", "test": "promtest_"},
	}, labelsMap)
	var count uint64 = 0
	var sum float64 = 0
	var max float64 = 0
	var min float64 = 0

	err = client.Scan(context.Background(), fmt.Sprintf("SELECT count(1), max(value), min(value), sum(value) FROM "+getSamplesTable()+
		" WHERE fingerprint IN (SELECT fingerprint "+
		"FROM "+serviceInfo.DatabaseNodeMap[0].Name+".time_series_v2 "+
		"WHERE JSONExtractString(labels, 'test') == 'promtest_%d') AND "+
		"  timestamp_ns > toUnixTimestamp(NOW() - INTERVAL '10 minute') * 1000000000 AND org_id=='1' AND value != 0", testID),
		nil, &count, &max, &min, &sum)
	if err != nil {
		t.Fatal(err)
	}

	var expectedSum float64 = 2756
	if count >= 65 {
		expectedSum = 3445
	}
	if count >= 78 {
		expectedSum = 4134
	}
	fmt.Printf("%d %f %f %f\n", count, min, max, sum)
	assert.Equal(t, 1., min)
	assert.Equal(t, 300., max)
	assert.True(t, math.Abs(expectedSum-sum) < 100)
}

type zipkinDoer struct {
	onSend chan error
}

func (z *zipkinDoer) Do(req *http.Request) (*http.Response, error) {
	req.Header.Set("X-Scope-OrgID", "1")
	client := &http.Client{}
	res, err := client.Do(req)
	z.onSend <- err
	return res, err
}

func testTempoZipkinIngest(t *testing.T, testid uint64) {
	err := ingestTestZipkinSpan(testid, "http://localhost:3215/tempo/spans")
	if err != nil {
		logger.Info("testTempoZipkinIngest Error", err.Error())
		t.Fatal(err)
	}
	fmt.Println(checkZipkinSpan(testid))
	fmt.Println("Send /api/v2/spans")
	err = ingestTestZipkinSpan(testid+1, "http://localhost:3215/api/v2/spans")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(checkZipkinSpan(testid + 1))
	fmt.Println("Send /api/v2/spans OK")
	fmt.Println("Sending 10MB spans")
	trace := `{"traceId":"%016x0000000000000000","name":"request_received","id":"%016x","timestamp":%d,"duration":343379,"localEndpoint":{"serviceName":"dummy-server"},"tags":{"job":"dummy-server","entity":"Shmi Skywalker_olive","http.status_code":"200","otel.status_code":"OK","service.name":"dummy-server","telemetry.sdk.language":"nodejs","telemetry.sdk.name":"opentelemetry","telemetry.sdk.version":"1.5.0"}}`
	traces := []string{}
	length := 0
	j := testid + 2
	for length < 10*1024*1024 {
		_trace := fmt.Sprintf(trace, j, j, time.Now().UnixMicro())
		traces = append(traces, _trace)
		j++
		length += len(_trace)
	}
	req, err := http.NewRequest("POST", "http://localhost:3215/tempo/spans", bytes.NewReader([]byte(
		"["+strings.Join(traces, ",")+"]")))
	if err != nil {
		panic(err)
	}
	req.Header.Set("X-Scope-OrgID", "1")
	req.ContentLength = 0
	client := http.Client{}
	resp, err := client.Do(req)
	fmt.Println("Sending 10MB spans Done")
	if err != nil {
		panic(err)
	}
	if resp.StatusCode/100 != 2 {
		bResp, _ := io.ReadAll(resp.Body)
		panic(fmt.Sprintf("[%d]: %s", resp.StatusCode, string(bResp)))
	}
	for k := testid + 2; k < j; k += 1000 {
		testIDs := []uint64{}
		for l := k; l < j && l < k+1000; l++ {
			testIDs = append(testIDs, l)
		}
		payloads := checkZipkinSpan(testIDs...)
		for l, p := range payloads {
			if p != traces[k+uint64(l)-testid-2] {
				panic(fmt.Sprintf("trace %s != %s", p, traces[k+uint64(l)-testid-2]))
			}
		}

	}
	fmt.Println("Sending 10MB spans Ok")
}

func ingestTestZipkinSpan(traceId uint64, url string) error {
	start := time.Now()
	onSend := make(chan error)
	reporter := zipkin.NewReporter(url, zipkin.Client(&zipkinDoer{onSend}))
	defer func() {
		close(onSend)
	}()
	defer reporter.Close()
	reporter.Send(model.SpanModel{
		SpanContext: model.SpanContext{
			TraceID: model.TraceID{
				High: traceId,
				Low:  0,
			},
			ID: model.ID(traceId),
		},
		Name:      "testspan1",
		Timestamp: start,
		Duration:  1000,
		Shared:    false,
		LocalEndpoint: &model.Endpoint{
			ServiceName: "service1",
			IPv4:        net.IPv4(192, 168, 0, 1),
			IPv6:        nil,
			Port:        8080,
		},
		Annotations: []model.Annotation{
			{Timestamp: start, Value: "annotation1"},
		},
		Tags: map[string]string{
			"test_id": strconv.FormatUint(traceId, 10),
		},
	})
	return <-onSend
}

func checkZipkinSpan(traceIDs ...uint64) []string {
	strTraceIDs := make([]string, len(traceIDs))
	strSpanIDs := make([]string, len(traceIDs))
	for i, traceID := range traceIDs {
		strTraceIDs[i] = fmt.Sprintf("%016x0000000000000000", traceID)
		strSpanIDs[i] = fmt.Sprintf("%016x", traceID)
	}
	//client, err := adapter.NewClient(context.Background(), &config.Cloki.Setting.DATABASE_DATA[0], true)
	client, err := chwrapper.NewSmartDatabaseAdapter(&config.Cloki.Setting.DATABASE_DATA[0], true)
	if err != nil {
		panic(err)
	}
	q := fmt.Sprintf(
		"SELECT payload FROM test.tempo_traces "+
			"WHERE lowerUTF8(hex(trace_id)) IN (%s) AND lowerUTF8(hex(span_id)) IN (%s) and oid = '1' "+
			"ORDER BY timestamp_ns ASC, trace_id asc",
		fmt.Sprintf("'%s'", strings.Join(strTraceIDs[:], "','")),
		fmt.Sprintf("'%s'", strings.Join(strSpanIDs[:], "','")),
	)
	res, err := client.GetList(q)
	if err != nil {
		panic(err)
	}
	if len(res) != len(traceIDs) {
		panic(fmt.Sprintf("COUNT mismatch: %d != %d", len(res), len(traceIDs)))
	}
	return res
}

func miscTest() {
	for _, url := range []string{"http://localhost:3215/metrics", "http://localhost:3215/config",
		"http://localhost:3215/ready"} {
		resp, err := http.Get(url)
		if err != nil {
			panic(err)
		}
		if resp.StatusCode/100 != 2 {
			body, _ := io.ReadAll(resp.Body)
			panic(fmt.Sprintf("miscTest: [%d]: %s", resp.StatusCode, string(body)))
		}
	}

}

func ingestInfluxTest(t *testing.T, testId uint64) {
	fmt.Println("GENERATING 5 mb influx")
	testLine := `logs,tag1=val1,test_id=%d,format=influx,type=logs message="%s",value=%d %d`
	lines := []string{}
	length := 0
	logsCnt := 0
	startTS := time.Now().UnixNano()
	logsSize := 0
	for length < 5*1024*1024 {
		msg := fmt.Sprintf("this is a very very long test string #%d", logsCnt)
		line := fmt.Sprintf(testLine, testId, msg, logsCnt, time.Now().UnixNano())
		lines = append(lines, line)
		length += len(line)
		logsCnt++
		logsSize += len(msg)
	}
	testMetricsLine := `logs,tag1=val1,test_id=%d,format=influx,type=metrics metric1=%d,metric2=%d %d`
	metricsCnt := 0
	for length < 10*1024*1024 {
		line := fmt.Sprintf(testMetricsLine, testId, metricsCnt, metricsCnt+1, time.Now().UnixNano())
		lines = append(lines, line)
		length += len(line)
		metricsCnt++
	}
	endTS := time.Now().UnixNano()
	fmt.Printf("SENDING 10 mb influx (%d logs)\n", logsSize)
	req, err := http.NewRequest("POST", "http://localhost:3215/influx/api/v2/write",
		bytes.NewReader([]byte(strings.Join(lines, "\r\n"))))
	if err != nil {
		panic(err)
	}
	req.Header.Set("X-Scope-OrgID", "1")
	req.ContentLength = 0
	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	if resp.StatusCode/100 != 2 {
		panic(fmt.Sprintf("[%d]: %s", resp.StatusCode, string(readAllNoErr(resp.Body))))
	}
	fmt.Println("CHECKING 10 mb influx")

	//CHClient, err := adapter.NewClient(context.Background(), &config.Cloki.Setting.DATABASE_DATA[0], true)
	CHClient, err := chwrapper.NewSmartDatabaseAdapter(&config.Cloki.Setting.DATABASE_DATA[0], true)
	if err != nil {
		panic(err)
	}
	var fp uint64
	err = CHClient.GetFirst(fmt.Sprintf(`SELECT fingerprint FROM time_series_gin_v2 WHERE
	(key, val) IN (('format', 'influx') as c1, ('test_id', '%s') as c2, ('type', 'logs') as c3) AND org_id = '1' GROUP BY fingerprint HAVING
	sum(((key, val) == c1) + ((key, val) == c2) * 2 + ((key, val) == c3) * 4) == 7`, strconv.FormatUint(testId, 10)),
		&fp)
	if err != nil {
		panic(err)
	}

	rows, err := CHClient.GetList(fmt.Sprintf(`SELECT formatRow('TSV', string, value) FROM samples_v4
WHERE fingerprint = %d AND org_id = '1' AND timestamp_ns >= %d AND timestamp_ns <= %d ORDER BY timestamp_ns ASC`,
		fp, startTS, endTS))
	if err != nil {
		panic(err)
	}

	j := 0
	for _, row := range rows {
		row = strings.Trim(row, " \t\r\n")
		expected := fmt.Sprintf("message=\"this is a very very long test string #%d\" value=%d\t0", j, j)
		if row != expected {
			panic(fmt.Sprintf("influx error: `%s` != `%s`", row, expected))
		}
		j++
	}
	if j != logsCnt {
		t.Fatalf("inclux error: ingested strings number %d != %d", j, logsCnt)
	}

	for add, metricName := range []string{"metric1", "metric2"} {
		err := CHClient.GetFirst(fmt.Sprintf(`SELECT fingerprint FROM time_series_gin_v2 WHERE
		(key, val) IN (('format', 'influx') as c1, ('test_id', '%s') as c2, ('type', 'metrics') as c3, ('__name__', '%s') as c4) AND org_id = '1' GROUP BY fingerprint HAVING
		sum(((key, val) == c1) + ((key, val) == c2) * 2 + ((key, val) == c3) * 4 + ((key, val) == c4) * 8) == 15`,
			strconv.FormatUint(testId, 10), metricName), &fp)
		if err != nil {
			panic(err)
		}

		rows, err = CHClient.GetList(fmt.Sprintf(`SELECT formatRow('TSV', string, value) FROM samples_v4
		WHERE fingerprint = %d AND org_id = '1' AND timestamp_ns >= %d AND timestamp_ns <= %d ORDER BY timestamp_ns ASC`,
			fp, startTS, endTS))
		if err != nil {
			panic(err)
		}
		j = 0
		for _, row := range rows {
			row = strings.Trim(row, " \t\r\n")
			expected := fmt.Sprintf("%d", j+add)
			if row != expected {
				panic(fmt.Sprintf("influx error: `%s` != `%s`", row, expected))
			}
			j++
		}
		if j != metricsCnt {
			t.Fatalf("inclux error: ingested strings number %d != %d", j, logsCnt)
		}
	}
	fmt.Println("SENDING 10 mb influx OK")
}

func readAllNoErr(reader io.Reader) []byte {
	res, _ := io.ReadAll(reader)
	return res
}

func testUsageCounting(t *testing.T) {
	fmt.Println("TESTING USAGE COUNTING")
	//CHClient, err := adapter.NewClient(context.Background(), &config.Cloki.Setting.DATABASE_DATA[0], true)
	client, err := chwrapper.NewSmartDatabaseAdapterWithDSN(config.Cloki.Setting.AnalyticsDatabase, true)
	if err != nil {
		panic(err)
	}
	data_client, err := chwrapper.NewSmartDatabaseAdapter(&config.Cloki.Setting.DATABASE_DATA[0], true)
	if err != nil {
		panic(err)
	}
	var org_stats [2][4]uint64
	var org_real_stats [2][4]uint64
	var orgids [2]string = [2]string{"0", "1"}
	for i := range 2 {
		err = client.Scan(context.Background(), `
SELECT 
    sum(logs_bytes_written), 
    sum(metrics_bytes_written),
    sum(traces_bytes_written), 
    bitmapCardinality(groupBitmapOrState(fingerprints_written))
FROM writer_usage_agg 
WHERE org_id = $1`, []any{orgids[i]}, &org_stats[i][0], &org_stats[i][1], &org_stats[i][2], &org_stats[i][3])
		if err != nil {
			panic(err)
		}
		fmt.Printf("Org %s: logs=%d, metrics=%d, traces=%d, fingerprints=%d\n",
			orgids[i], org_stats[i][0], org_stats[i][1], org_stats[i][2], org_stats[i][3])

		err = data_client.Scan(context.Background(),
			"SELECT sum(length(string)+16) from samples_v4 WHERE string != '' AND org_id = $1",
			[]any{orgids[i]},
			&org_real_stats[i][0])
		if err != nil {
			panic(err)
		}
		err = data_client.Scan(context.Background(),
			"SELECT count() * 24 from samples_v4 WHERE string == '' AND org_id = $1",
			[]any{orgids[i]},
			&org_real_stats[i][1])
		if err != nil {
			panic(err)
		}
		err = data_client.Scan(context.Background(),
			"SELECT sum(length(payload)) from tempo_traces WHERE oid = $1",
			[]any{orgids[i]},
			&org_real_stats[i][2])
		if err != nil {
			panic(err)
		}
		err = data_client.Scan(context.Background(),
			"SELECT count(distinct fingerprint) from time_series_v2 WHERE org_id = $1",
			[]any{orgids[i]},
			&org_real_stats[i][3])
		if err != nil {
			panic(err)
		}
		fmt.Printf("Org %s: real_logs=%d, real_metrics=%d, real_traces=%d, real_fingerprints=%d\n",
			orgids[i], org_real_stats[i][0], org_real_stats[i][1], org_real_stats[i][2], org_real_stats[i][3])
		for j := 0; j < 4; j++ {
			if uint64(float64(org_stats[i][j])*0.9) > org_real_stats[i][j] {
				t.Fatalf("Org %s: stats mismatch at %d: expected %d, got %d", orgids[i], j, org_real_stats[i][j], org_stats[i][j])
			}
		}
	}
}
