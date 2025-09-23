package shared

import (
	"context"
	"time"

	"github.com/metrico/qryn/reader/model"
	"github.com/metrico/qryn/reader/utils/dbVersion"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
)

const SAMPLES_TYPE_LOGS = 1
const SAMPLES_TYPE_METRICS = 2
const SAMPLES_TYPE_BOTH = 0

type RequestProcessor interface {
	IsMatrix() bool
	Process(*PlannerContext, chan []LogEntry) (chan []LogEntry, error)
}

type RandomFilter struct {
	Max int
	I   int
}

type PlannerContext struct {
	IsCluster bool
	From      time.Time
	To        time.Time
	OrderASC  bool
	Limit     int64
	Offset    time.Duration

	TimeSeriesGinTableName  string
	SamplesTableName        string
	TimeSeriesTableName     string
	TimeSeriesDistTableName string
	Metrics15sTableName     string
	PatternsTable           string

	TracesAttrsTable     string
	TracesAttrsDistTable string
	TracesTable          string
	TracesDistTable      string
	TracesKVTable        string
	TracesKVDistTable    string

	ProfilesSeriesGinTable     string
	ProfilesSeriesGinDistTable string
	ProfilesTable              string
	ProfilesDistTable          string
	ProfilesSeriesTable        string
	ProfilesSeriesDistTable    string
	PatternsDistTable          string

	UseCache bool

	Ctx context.Context

	CHDb       model.ISqlxDB
	CHFinalize bool
	CHSqlCtx   *sql.Ctx

	DDBSamplesTable string
	DDBTSTable      string

	CancelCtx context.CancelFunc

	Step time.Duration

	DeleteID string

	Type uint8

	id int

	RandomFilter   RandomFilter
	CachedTraceIds []string
	VersionInfo    dbversion.VersionInfo
}

func (p *PlannerContext) Id() int {
	p.id++
	return p.id
}

type SQLRequestPlanner interface {
	Process(ctx *PlannerContext) (sql.ISelect, error)
}

type LogEntry struct {
	TimestampNS int64
	Fingerprint uint64
	Labels      map[string]string
	Message     string
	Value       float64

	Err error
}

type RequestProcessorChain []RequestProcessor

type RequestPlanner interface {
	Process(cnain RequestProcessorChain) (RequestProcessorChain, error)
}
