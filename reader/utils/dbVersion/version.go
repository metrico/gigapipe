package dbversion

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metrico/qryn/v4/reader/model"
	"github.com/metrico/qryn/v4/shared/distconfig"
)

type VersionInfo map[string]int64

func (v VersionInfo) IsVersionSupported(ver string, fromNS int64, toNS int64) bool {
	time, ok := v[ver]
	fmt.Printf("Checking %d - %d", fromNS, time)
	return ok && (fromNS >= (time * 1000000000))
}

// CapStaleness is the server capability key for ORDER BY ... WITH FILL STALENESS,
// which clickhouse added in 24.11. Below that the clause is a parse error, so it
// must never be emitted; callers fall back to arrayJoin range expansion instead.
const CapStaleness = "ch_staleness"

// HasCapability reports a server capability, as opposed to IsVersionSupported,
// which reports whether a qryn migration was applied before a data range. A
// capability is not scoped to a time range: it is a property of the running
// server, present or absent, so it carries no timestamp comparison.
func (v VersionInfo) HasCapability(name string) bool {
	_, ok := v[name]
	return ok
}

// chStalenessMinMajor / chStalenessMinMinor is the first clickhouse release that
// parses WITH FILL STALENESS (24.11).
const chStalenessMinMajor = 24
const chStalenessMinMinor = 11

// supportsStaleness parses a clickhouse version() string like "24.9.1.3278" or
// "25.3.14.14" and reports whether it is at least 24.11.
func supportsStaleness(version string) bool {
	parts := strings.SplitN(version, ".", 3)
	if len(parts) < 2 {
		return false
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return false
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return false
	}
	if major != chStalenessMinMajor {
		return major > chStalenessMinMajor
	}
	return minor >= chStalenessMinMinor
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
	rows, err := db.QueryCtx(ctx, fmt.Sprintf(`SELECT argMax(name, inserted_at) as _name , argMax(value, inserted_at) as _value 
FROM %s WHERE type='update' GROUP BY fingerprint HAVING _name!=''`, tableName))
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
	metrics15sV1 := false
	for tables.Next() {
		var tableName string
		err = tables.Scan(&tableName)
		if err != nil {
			fmt.Println(err)
			continue
		}
		metrics15sV1 = metrics15sV1 || tableName == "metrics_15s" || tableName == "metrics_15s_dist"
	}
	if !metrics15sV1 {
		_versions["v5"] = 0
	}

	// Probe the server for optional SQL features. A failed probe leaves the
	// capability absent, which degrades to the compatible path rather than an
	// error, so a transient error here is not fatal to the query.
	if verRows, err := db.QueryCtx(ctx, `SELECT version()`); err == nil {
		var serverVersion string
		if verRows.Next() {
			if err := verRows.Scan(&serverVersion); err == nil && supportsStaleness(serverVersion) {
				_versions[CapStaleness] = 0
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
