package model

func (TableSample) TableName() string {
	return "samples"
}

func (TableSample) TableEngine() string {
	return "MergeTree    PARTITION BY toDate(timestamp_ns / 1000)    ORDER BY (fingerprint, timestamp_ms);"
}

// swagger:model CreateUserStruct
type TableSample struct {
	FingerPrint uint64 `db:"fingerprint" clickhouse:"type:UInt64" json:"fingerprint"`
	// required: true
	TimestampNS int64 `db:"timestamp_ns" clickhouse:"type:Int64" json:"timestamp_ns"`
	//
	Value float64 `db:"value" clickhouse:"type:Float64" json:"value"`
	// example: 10
	// required: true
	String string `db:"string" clickhouse:"type:String" json:"string"`
}

type TableMetrics struct {
	FingerPrint uint64 `db:"fingerprint" clickhouse:"type:UInt64" json:"fingerprint"`
	// required: true
	TimestampNS int64 `db:"timestamp_ns" clickhouse:"type:Int64" json:"timestamp_ns"`
	//
	Value float64 `db:"value" clickhouse:"type:Float64" json:"value"`
}

/*
CREATE TABLE cloki.samples
(
    `fingerprint` UInt64,
    `timestamp_ms` Int64,
    `value` Float64,
    `string` String
)
ENGINE = MergeTree
PARTITION BY toRelativeHourNum(toDateTime(timestamp_ms / 1000))
ORDER BY (fingerprint, timestamp_ms)
TTL toDateTime(timestamp_ms / 1000) + toIntervalDay(7)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600

*/
