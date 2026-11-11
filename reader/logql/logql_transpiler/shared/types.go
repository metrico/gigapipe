package shared

import (
	"context"
	"time"

	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/utils/dbVersion"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
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
	// SpansPerSpanSet caps the spans listed per TraceQL search spanset; 0 means the default.
	SpansPerSpanSet int64
	// AncestryRounds is the pointer-jumping rounds of TraceQL structural
	// operators; 0 means the default.
	AncestryRounds int

	TimeSeriesGinTableName     string
	TimeSeriesGinDistTableName string
	SamplesTableName           string
	SamplesDistTableName       string
	TimeSeriesTableName        string
	TimeSeriesDistTableName    string
	Metrics15sTableName        string
	Metrics15sDistTableName    string
	PatternsTable              string

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
	// Instant marks an instant query, evaluated at To.
	Instant bool
	// Grid is the evaluation grid of a range aggregation on the Go path.
	Grid *EvalGrid

	DeleteID string

	Type uint8

	id int

	RandomFilter RandomFilter
	// Portions is the number of complexity portions the request was read in,
	// or 0 when it was read at once. It is final when the planner's Process returns.
	Portions int64
	// TraceQL is the request's query text, for its log lines.
	TraceQL     string
	VersionInfo dbversion.VersionInfo

	// TracesIntrinsicWhere holds extra WHERE conditions for the tempo_traces table
	// (e.g., nestedSetParent < 0 → parent_id = '').
	TracesIntrinsicWhere []sql.SQLCondition

	// TracesOperandSpans, when set, holds the previous pipeline stage's span
	// rows: the only ones TraceQL selectors may match, and the input of a
	// nested pipeline.
	TracesOperandSpans *sql.With

	// TracesSample, when set, keeps the sampled rows of every TraceQL
	// operand read.
	TracesSample sql.SQLCondition
}

// TotalJobs is the job count of a search response: Portions, or 1 when the request was read at once.
func (p *PlannerContext) TotalJobs() int64 {
	return max(p.Portions, 1)
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
