package model

import (
	"github.com/metrico/qryn/writer/ch_wrapper"
	"time"
)

type InsertServiceOpts struct {
	//Session     IChClientFactory
	Session        ch_wrapper.IChClientFactory
	Node           *DataDatabasesMap
	Interval       time.Duration
	MaxQueueSize   int64
	OnBeforeInsert func()
	ParallelNum    int
	AsyncInsert    bool
}
