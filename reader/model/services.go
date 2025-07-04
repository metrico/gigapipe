package model

import (
	"context"
	"github.com/jmoiron/sqlx"
	"time"
)

type ITempoService interface {
	Query(ctx context.Context, startNS int64, endNS int64, traceId []byte, binIds bool) (chan *SpanResponse, error)
	Tags(ctx context.Context) (chan string, error)
	Values(ctx context.Context, tag string) (chan string, error)
	ValuesV2(ctx context.Context, key string, query string, from time.Time, to time.Time, limit int) (chan string, error)
	Search(ctx context.Context, tags string, minDurationNS int64, maxDurationNS int64,
		limit int, fromNS int64, toNS int64) (chan *TraceResponse, error)
	SearchTraceQL(ctx context.Context, q string, limit int, from time.Time, to time.Time,
		step time.Duration) (*TraceQLResponse, error)
	TagsV2(ctx context.Context, query string, from time.Time, to time.Time, limit int) (chan string, error)
}

type IQueryLabelsService interface {
	Labels(ctx context.Context, startMs int64, endMs int64, labelsType uint16) (chan string, error)
	PromValues(ctx context.Context, label string, match []string, startMs int64, endMs int64,
		labelsType uint16) (chan string, error)
	Prom2LogqlMatch(match string) (string, error)
	Values(ctx context.Context, label string, match []string, startMs int64, endMs int64,
		labelsType uint16) (chan string, error)
	Series(ctx context.Context, requests []string, startMs int64, endMs int64,
		labelsType uint16) (chan string, error)
}
type IQueryRangeService interface {
	QueryRange(ctx context.Context, query string, fromNs int64, toNs int64, stepMs int64,
		limit int64, forward bool) (chan QueryRangeOutput, error)
	QueryInstant(ctx context.Context, query string, timeNs int64, stepMs int64,
		limit int64) (chan QueryRangeOutput, error)
	Tail(ctx context.Context, query string) (IWatcher, error)
}

// Service : here you tell us what Salutation is
type ServiceData struct {
	Session      IDBRegistry
	lastPingTime time.Time
}

func (s *ServiceData) Ping() error {
	return s.Session.Ping()
}

// ServiceConfig
type ServiceConfig struct {
	Session *sqlx.DB
}

// ServiceConfigDatabases
type ServiceConfigDatabases struct {
	Session map[string]*sqlx.DB
}
