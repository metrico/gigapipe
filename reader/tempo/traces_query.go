package tempo

import (
	"context"

	"github.com/metrico/qryn/v4/reader/plugins"
	sql "github.com/metrico/qryn/v4/reader/utils/sql_select"
	"github.com/metrico/qryn/v4/reader/utils/tables"
)

func GetTracesQuery(ctx context.Context, idx *SQLIndexQuery, limit int, fromNS int64, toNS int64,
	distributed bool, minDurationNS int64, maxDurationNS int64) (sql.ISelect, error) {
	p := plugins.GetGetTracesQueryPlugin()
	if p != nil {
		return (*p)(ctx, idx, limit, fromNS, toNS, distributed, minDurationNS, maxDurationNS)
	}
	tableName := tables.GetTableName("tempo_traces")
	if distributed {
		tableName = tables.GetTableName("tempo_traces_dist")
	}
	query := sql.NewSelect().Select(
		sql.NewRawObject("hex(trace_id)"),
		sql.NewCol(sql.NewRawObject("service_name"), "root_service_name"),
		sql.NewCol(sql.NewRawObject("name"), "root_trace_name"),
		sql.NewCol(sql.NewRawObject("timestamp_ns"), "start_time_unix_nano"),
		sql.NewCol(sql.NewRawObject("intDiv(duration_ns, 1000000)"),
			"duration_ms"),
	).From(sql.NewRawObject(tableName))
	//TODO: move to PRO !TURNED OFF .AndWhere(sql.Eq(sql.NewRawObject("oid"), sql.NewStringVal(oid)))
	if idx != nil {
		query.AndWhere(sql.NewIn(sql.NewRawObject("(trace_id, span_id)"), idx))
	}
	if fromNS > 0 {
		query.AndWhere(sql.Gt(sql.NewRawObject("start_time_unix_nano"), sql.NewIntVal(fromNS)))
	}
	if toNS > 0 {
		query.AndWhere(sql.Le(sql.NewRawObject("start_time_unix_nano"), sql.NewIntVal(toNS)))
	}
	if minDurationNS > 0 {
		query.AndWhere(sql.Gt(sql.NewRawObject("duration_ms"), sql.NewIntVal(minDurationNS/1e6)))
	}
	if maxDurationNS > 0 {
		query.AndWhere(sql.Le(sql.NewRawObject("duration_ms"), sql.NewIntVal(maxDurationNS/1e6)))
	}
	if limit > 0 {
		query.Limit(sql.NewIntVal(int64(limit)))
	}
	query.OrderBy(sql.NewRawObject("start_time_unix_nano DESC"))
	return query, nil
}
