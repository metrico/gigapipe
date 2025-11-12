package insert

import (
	"fmt"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/metrico/qryn/v4/writer/model"
	"github.com/metrico/qryn/v4/writer/service"
	"github.com/metrico/qryn/v4/writer/utils/logger"
)

const (
	metadataType = "metric_metadata"
)

type MetadataAcquirer struct {
	MetricName  *service.PooledColumn[*proto.ColStr]
	Value       *service.PooledColumn[*proto.ColStr]
	TimestampMS *service.PooledColumn[proto.ColInt64]
}

func (a *MetadataAcquirer) acq() *MetadataAcquirer {
	service.StartAcq()
	defer service.FinishAcq()
	a.MetricName = service.StrPool.Acquire("metric_name")
	a.Value = service.StrPool.Acquire("value")
	a.TimestampMS = service.Int64Pool.Acquire("timestamp_ms")
	return a
}

func (a *MetadataAcquirer) serialize() []service.IColPoolRes {
	return []service.IColPoolRes{a.MetricName, a.Value, a.TimestampMS}
}

func (a *MetadataAcquirer) deserialize(res []service.IColPoolRes) *MetadataAcquirer {
	a.MetricName, a.Value, a.TimestampMS =
		res[0].(*service.PooledColumn[*proto.ColStr]),
		res[1].(*service.PooledColumn[*proto.ColStr]),
		res[2].(*service.PooledColumn[proto.ColInt64])
	return a
}

func NewMetadataInsertService(opts model.InsertServiceOpts) service.IInsertServiceV2 {
	if opts.ParallelNum <= 0 {
		opts.ParallelNum = 1
	}

	table := "metrics_meta"
	if opts.Node.ClusterName != "" {
		table += "_dist"
	}
	insertReq := fmt.Sprintf("INSERT INTO %s (metric_name, value, timestamp_ms)", table)

	return &service.InsertServiceV2Multimodal{
		ServiceData:    service.ServiceData{},
		V3Session:      opts.Session,
		DatabaseNode:   opts.Node,
		PushInterval:   opts.Interval,
		SvcNum:         opts.ParallelNum,
		AsyncInsert:    opts.AsyncInsert,
		InsertRequest:  insertReq,
		ServiceType:    "metadata",
		MaxQueueSize:   opts.MaxQueueSize,
		OnBeforeInsert: opts.OnBeforeInsert,
		AcquireColumns: func() []service.IColPoolRes {
			return (&MetadataAcquirer{}).acq().serialize()
		},
		ProcessRequest: func(ts any, res []service.IColPoolRes) (int, []service.IColPoolRes, error) {
			metadataData, ok := ts.(*model.MetadataData)
			if !ok {
				logger.Info("invalid request type metadata")
				return 0, nil, fmt.Errorf("invalid request type metadata")
			}
			acquirer := (&MetadataAcquirer{}).deserialize(res)
			_len := len(acquirer.MetricName.Data)

			for _, metricName := range metadataData.MetricNames {
				acquirer.MetricName.Data.Append(metricName)
			}
			for i := 0; i < len(metadataData.MetricNames); i++ {
				if i < len(metadataData.MetadataJSON) {
					acquirer.Value.Data.Append(metadataData.MetadataJSON[i])
				} else {
					acquirer.Value.Data.Append("{}")
				}
			}
			// Use Int64 for timestamp_ms (milliseconds since Unix epoch)
			// This works perfectly with ch-go native protocol otherwise DateTime64(9, 'UTC') converted to Int64
			timestampMS := time.Now().UnixMilli()
			for i := 0; i < len(metadataData.MetricNames); i++ {
				acquirer.TimestampMS.Data.Append(timestampMS)
			}

			return len(acquirer.MetricName.Data) - _len, res, nil
		},
	}
}
