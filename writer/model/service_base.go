package model

import (
	"time"

	"github.com/metrico/qryn/writer/chwrapper"
)

type InsertServiceOpts struct {
	//Session     IChClientFactory
	Session        chwrapper.IChClientFactory
	Node           *DataDatabasesMap
	Interval       time.Duration
	MaxQueueSize   int64
	OnBeforeInsert func()
	ParallelNum    int
	AsyncInsert    bool
}
