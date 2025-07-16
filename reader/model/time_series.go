package model

import "time"

func (TableTimeSeries) TableName() string {
	return "time_series"
}

func (TableTimeSeries) TableEngine() string {
	return "ReplacingMergeTree PARTITION BY date    ORDER BY fingerprint;"
}

type TableTimeSeries struct {
	Date time.Time `db:"date" clickhouse:"type:Date" json:"date"`
	// required: true
	FingerPrint uint64 `db:"fingerprint" clickhouse:"type:UInt64" json:"fingerprint"`
	//
	Labels string `db:"labels" clickhouse:"type:String" json:"value"`
	// example: 10
	// required: true
	Name string `db:"name" clickhouse:"type:String" json:"string"`
}

/*
CREATE TABLE cloki.time_series
(
    `date` Date,
    `fingerprint` UInt64,
    `labels` String,
    `name` String
)
ENGINE = ReplacingMergeTree(date)
PARTITION BY date
ORDER BY fingerprint
TTL date + toIntervalDay(7)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600
*/
