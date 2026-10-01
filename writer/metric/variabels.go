package metric

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	JsonParseErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "json_parse_errors_count",
		Help: "The total number of JSON parse errors",
	})
	ConnectionResetByPeer = promauto.NewCounter(prometheus.CounterOpts{
		Name: "connection_reset_by_peer_count",
		Help: "The total number of connections reset by peer",
	})
	IngestRejected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ingest_rejected_count",
		Help: "The total number of ingested items dropped, by reason",
	}, []string{"reason"})
	SentRows = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sent_rows",
		Help: "The total number of rows sent",
	}, []string{"service"})
	SentBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sent_bytes",
		Help: "The total number of bytes sent",
	}, []string{"service"})
	TxCloseTime = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "tx_close_time_ms",
		Help: "Transaction close time in milliseconds",
		//	Buckets: prometheus.LinearBuckets(100, 100, 6), // Start at 100, increment by 100, and create 6 buckets
		Buckets: []float64{100, 200, 500, 1000, 5000, 10000},
	})
	SendTime = promauto.NewSummary(prometheus.SummaryOpts{
		Name: "send_time_ms",
		Help: "Send time in milliseconds",
		Objectives: map[float64]float64{
			0.25: 0.02,
			0.5:  200,
			0.75: 200,
			0.90: 200}, // Error tolerance of +/- 200ms
	})
)
