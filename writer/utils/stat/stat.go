package stat

import (
	"math"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/metrico/qryn/writer/metric"
	"github.com/metrico/qryn/writer/utils/proto/prompb"
)

var metricsMtx = sync.Mutex{}

const timeSpanSec = 30

var sentMetrics = func() []map[string]int64 {
	res := make([]map[string]int64, timeSpanSec+2)
	for i := 0; i < 4; i++ {
		res[i] = make(map[string]int64, timeSpanSec+2)
	}
	return res
}()

var counters = map[string]int64{}

func getOrDefault(idx int64, name string, def int64) int64 {
	if _, ok := sentMetrics[idx][name]; !ok {
		return def
	}
	return sentMetrics[idx][name]
}

func getIdx(time2 time.Time) int64 {
	return time2.Unix() % (timeSpanSec + 2)
}

func checkMap(idx int64) {
	if sentMetrics[idx] == nil {
		sentMetrics[idx] = make(map[string]int64, 20)
	}
}

func AddSentMetrics(name string, count int64) {
	metricsMtx.Lock()
	defer metricsMtx.Unlock()
	idx := getIdx(time.Now())
	checkMap(idx)
	if _, ok := sentMetrics[idx][name]; !ok {
		sentMetrics[idx][name] = 0
	}
	sentMetrics[idx][name] = sentMetrics[idx][name] + count
	counters[name+"_counter"] += count
	// Define a map of metric handlers for different conditions
	metricHandlers := map[string]func(int64){
		"json_parse_errors": func(count int64) {
			metric.JsonParseErrors.Add(float64(count))
		},
		"connection_reset_by_peer": func(count int64) {
			metric.ConnectionResetByPeer.Add(float64(count))
		},
	}
	if strings.HasSuffix(name, "_sent_rows") {
		name = strings.Replace(name, "_sent_rows", "", -1)
		metric.SentRows.WithLabelValues(name).Add(float64(count))
	} else if strings.HasSuffix(name, "_sent_bytes") {
		name = strings.Replace(name, "_sent_bytes", "", -1)
		metric.SentBytes.WithLabelValues(name).Add(float64(count))
	} else if handler, exists := metricHandlers[name]; exists {
		handler(count)
	}
}

func AddCompoundMetric(name string, count int64) {
	metricsMtx.Lock()
	defer metricsMtx.Unlock()
	idx := getIdx(time.Now())
	checkMap(idx)
	max := getOrDefault(idx, name+"_max", math.MinInt64)
	if max < count {
		max = count
	}
	min := getOrDefault(idx, name+"_min", math.MaxInt)
	if min > count {
		min = count
	}
	sum := getOrDefault(idx, name+"_sum", 0) + count
	cnt := getOrDefault(idx, name+"_count", 0) + 1
	sentMetrics[idx][name+"_max"] = max
	sentMetrics[idx][name+"_min"] = min
	sentMetrics[idx][name+"_sum"] = sum
	sentMetrics[idx][name+"_count"] = cnt
	if strings.Contains(name, "tx_close_time_ms") {
		metric.TxCloseTime.Observe(float64(count)) // Adjust as needed for labeling
	} else {
		metric.SendTime.Observe(float64(count))
	}

}

func GetRate() map[string]int64 {
	metricsMtx.Lock()
	defer metricsMtx.Unlock()
	return getRate()
}

func getRate() map[string]int64 {
	end := time.Now()
	start := end.Add(time.Second * -31)
	res := make(map[string]int64, 100)
	for i := start; i.Before(end); i = i.Add(time.Second) {
		idx := getIdx(i)
		checkMap(idx)
		for k, v := range sentMetrics[idx] {
			if _, ok := res[k]; !ok {
				res[k] = v
				continue
			}
			if strings.HasSuffix(k, "_max") {
				if res[k] < v {
					res[k] = v
				}
				continue
			}
			if strings.HasSuffix(k, "_min") {
				if res[k] > v {
					res[k] = v
				}
				continue
			}
			res[k] += v
		}
	}

	for k, v := range counters {
		res[k] = v
	}
	return res
}

var nameSanitizer = regexp.MustCompile(`\W`)

func SanitizeName(name string) string {
	return strings.ToLower(nameSanitizer.ReplaceAllString(name, "_"))
}

func GetRemoteWrite() *prompb.WriteRequest {
	metricsMtx.Lock()
	defer metricsMtx.Unlock()
	req := prompb.WriteRequest{
		Timeseries: make([]*prompb.TimeSeries, 0, 50),
	}
	now := time.Now().UnixMilli() - 2000
	for k, v := range getRate() {
		ts := prompb.TimeSeries{
			Labels: []*prompb.Label{{
				Name:  "__name__",
				Value: SanitizeName(k),
			}},
			Samples: []*prompb.Sample{{Timestamp: now, Value: float64(v)}},
		}
		req.Timeseries = append(req.Timeseries, &ts)
	}
	for k, v := range counters {
		ts := prompb.TimeSeries{
			Labels: []*prompb.Label{{
				Name:  "__name__",
				Value: SanitizeName(k),
			}},
			Samples: []*prompb.Sample{{Timestamp: now, Value: float64(v)}},
		}
		req.Timeseries = append(req.Timeseries, &ts)
	}
	return &req
}

func ResetRate() {
	metricsMtx.Lock()
	defer metricsMtx.Unlock()
	idx := getIdx(time.Now().Add(time.Second))
	sentMetrics[idx] = make(map[string]int64, 20)
}
