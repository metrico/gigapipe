package dbversion

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/shared/distconfig"
)

type VersionInfo map[string]int64

func (v VersionInfo) IsVersionSupported(ver string, fromNS int64, toNS int64) bool {
	time, ok := v[ver]
	return ok && (fromNS >= (time * 1000000000))
}

// CapMetrics15s is the capability key set when the metrics_15s table (or its
// distributed wrapper) exists in the database.
const CapMetrics15s = "cap_metrics_15s"

// MarkerMetrics15s is the settings marker holding the unix time the metrics_15s
// aggregation pipeline was last enabled. Absent on installs that never toggled
// the pipeline; set to a far-future time while it is disabled.
const MarkerMetrics15s = "metrics_15s"

// Metrics15sAvailable reports whether metrics_15s can serve a query window
// starting at fromNS: the table must exist, and the window must not reach back
// before the pipeline was last enabled (older buckets were never materialized).
func (v VersionInfo) Metrics15sAvailable(fromNS int64) bool {
	if !v.HasCapability(CapMetrics15s) {
		return false
	}
	t, ok := v[MarkerMetrics15s]
	return !ok || fromNS >= t*1000000000
}

// MarkerMetrics15sStartCount is the settings marker holding the unix time of
// the first metrics_15s cell whose start_count the MV fills on every node; its
// largest value across rows counts.
const MarkerMetrics15sStartCount = "metrics_15s_start_count"

// Metrics15sStartCountFrom returns the first metrics_15s cell (epoch ns) whose
// start_count is filled, or math.MaxInt64 when no cell's is.
func (v VersionInfo) Metrics15sStartCountFrom() int64 {
	t, ok := v[MarkerMetrics15sStartCount]
	if !ok || t > math.MaxInt64/1000000000 {
		return math.MaxInt64
	}
	return t * 1000000000
}

// CapStaleness is the server capability key for ORDER BY ... WITH FILL STALENESS,
// which clickhouse added in 24.11. Below that the clause is a parse error, so it
// must never be emitted; callers fall back to arrayJoin range expansion instead.
const CapStaleness = "ch_staleness"

// HasCapability reports a server capability, as opposed to IsVersionSupported,
// which reports whether a gigapipe migration was applied before a data range. A
// capability is not scoped to a time range: it is a property of the running
// server, present or absent, so it carries no timestamp comparison.
func (v VersionInfo) HasCapability(name string) bool {
	_, ok := v[name]
	return ok
}

// CapStructuralQueries is the server capability key for TraceQL structural
// queries, which need recursive CTEs over the analyzer (clickhouse 24.9+).
const CapStructuralQueries = "ch_structural_queries"

// serverCapabilities lists the capabilities of a clickhouse version() string
// like "24.9.1.3278".
func serverCapabilities(version string) []string {
	var caps []string
	if versionAtLeast(version, 24, 9) {
		caps = append(caps, CapStructuralQueries)
	}
	if versionAtLeast(version, 24, 11) {
		caps = append(caps, CapStaleness)
	}
	return caps
}

// versionAtLeast reports whether a clickhouse version() string is at least
// major.minor. An unparseable version is never at least anything.
func versionAtLeast(version string, major, minor int) bool {
	parts := strings.SplitN(version, ".", 3)
	if len(parts) < 2 {
		return false
	}
	gotMajor, err := strconv.Atoi(parts[0])
	if err != nil {
		return false
	}
	gotMinor, err := strconv.Atoi(parts[1])
	if err != nil {
		return false
	}
	if gotMajor != major {
		return gotMajor > major
	}
	return gotMinor >= minor
}

var versions = make(map[string]VersionInfo, 10)
var mtx sync.Mutex
var throttled int32 = 0

func throttle() {
	if !atomic.CompareAndSwapInt32(&throttled, 0, 1) {
		return
	}
	go func() {
		time.Sleep(time.Second * 10)
		atomic.StoreInt32(&throttled, 0)
		mtx.Lock()
		versions = make(map[string]VersionInfo, 10)
		mtx.Unlock()
	}()
}

func GetVersionInfo(ctx context.Context, dist bool, db model.ISqlxDB) (VersionInfo, error) {
	mtx.Lock()
	ver, ok := versions[db.GetName()]
	mtx.Unlock()
	if ok {
		return ver, nil
	}
	tableName := "settings"
	if dist {
		tableName += distconfig.Suffix()
	}
	_versions := map[string]int64{}
	// The start_count marker takes the largest cutoff over its rows.
	rows, err := db.QueryCtx(ctx, fmt.Sprintf(`SELECT argMax(name, inserted_at) as _name ,
if(_name = '%s', toString(max(toInt64OrZero(value))), argMax(value, inserted_at)) as _value 
FROM %s WHERE type='update' GROUP BY fingerprint HAVING _name!=''`, MarkerMetrics15sStartCount, tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var ver, time string
		err = rows.Scan(&ver, &time)
		if err != nil {
			fmt.Println(err)
			continue
		}
		_time, err := strconv.ParseInt(time, 10, 64)
		if err == nil {
			_versions[ver] = _time
		}
	}

	tables, err := db.QueryCtx(ctx, `SHOW TABLES`)
	if err != nil {
		return nil, err
	}
	defer tables.Close()
	metrics15sPresent := false
	for tables.Next() {
		var tableName string
		err = tables.Scan(&tableName)
		if err != nil {
			fmt.Println(err)
			continue
		}
		metrics15sPresent = metrics15sPresent || tableName == "metrics_15s" || tableName == "metrics_15s_dist"
	}
	if metrics15sPresent {
		_versions[CapMetrics15s] = 0
	}

	// Probe the server for optional SQL features. A failed probe leaves the
	// capability absent, which degrades to the compatible path rather than an
	// error, so a transient error here is not fatal to the query.
	if verRows, err := db.QueryCtx(ctx, `SELECT version()`); err == nil {
		var serverVersion string
		if verRows.Next() {
			if err := verRows.Scan(&serverVersion); err == nil {
				for _, c := range serverCapabilities(serverVersion) {
					_versions[c] = 0
				}
			}
		}
		verRows.Close()
	}

	mtx.Lock()
	versions[db.GetName()] = _versions
	mtx.Unlock()
	throttle()
	return _versions, nil
}
