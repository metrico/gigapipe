package dbversion

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metrico/qryn/reader/model"
)

type VersionInfo map[string]int64

func (v VersionInfo) IsVersionSupported(ver string, fromNS int64, toNS int64) bool {
	time, ok := v[ver]
	fmt.Printf("Checking %d - %d", fromNS, time)
	return ok && (fromNS >= (time * 1000000000))
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
		tableName += "_dist"
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
	mtx.Lock()
	versions[db.GetName()] = _versions
	mtx.Unlock()
	throttle()
	return _versions, nil
}
