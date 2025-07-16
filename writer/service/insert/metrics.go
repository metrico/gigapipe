package insert

import (
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/metrico/qryn/writer/model"
	"github.com/metrico/qryn/writer/plugins"
	"github.com/metrico/qryn/writer/service"
	"github.com/metrico/qryn/writer/utils/logger"
)

type MetricsAcquirer struct {
	Type        *service.PooledColumn[proto.ColUInt8]
	Fingerprint *service.PooledColumn[proto.ColUInt64]
	TimestampNS *service.PooledColumn[proto.ColInt64]
	Value       *service.PooledColumn[proto.ColFloat64]
}

func (a *MetricsAcquirer) acq() *MetricsAcquirer {
	service.StartAcq()
	defer service.FinishAcq()
	a.Type = service.UInt8Pool.Acquire("type")
	a.Fingerprint = service.UInt64Pool.Acquire("fingerprint")
	a.TimestampNS = service.Int64Pool.Acquire("timestamp_ns")
	a.Value = service.Float64Pool.Acquire("value")
	return a
}

func (a *MetricsAcquirer) serialize() []service.IColPoolRes {
	return []service.IColPoolRes{a.Type, a.Fingerprint, a.TimestampNS, a.Value}
}

func (a *MetricsAcquirer) deserialize(res []service.IColPoolRes) *MetricsAcquirer {
	a.Type, a.Fingerprint, a.TimestampNS, a.Value =
		res[0].(*service.PooledColumn[proto.ColUInt8]),
		res[1].(*service.PooledColumn[proto.ColUInt64]),
		res[2].(*service.PooledColumn[proto.ColInt64]),
		res[3].(*service.PooledColumn[proto.ColFloat64])
	return a
}

func NewMetricsInsertService(opts model.InsertServiceOpts) service.IInsertServiceV2 {

	plugin := plugins.GetMetricInsertServicePlugin()
	if plugin != nil {
		return (*plugin)(opts)
	}

	if opts.ParallelNum <= 0 {
		opts.ParallelNum = 1
	}
	tableName := "samples_v3"
	if opts.Node.ClusterName != "" {
		tableName += "_dist"
	}
	insertReq := fmt.Sprintf("INSERT INTO %s (`type`, fingerprint, timestamp_ns, value)",
		tableName)

	return &service.InsertServiceV2Multimodal{
		ServiceData:    service.ServiceData{},
		V3Session:      opts.Session,
		DatabaseNode:   opts.Node,
		PushInterval:   opts.Interval,
		SvcNum:         opts.ParallelNum,
		AsyncInsert:    opts.AsyncInsert,
		InsertRequest:  insertReq,
		ServiceType:    "metrics",
		MaxQueueSize:   opts.MaxQueueSize,
		OnBeforeInsert: opts.OnBeforeInsert,
		AcquireColumns: func() []service.IColPoolRes {
			return (&MetricsAcquirer{}).acq().serialize()
		},
		ProcessRequest: func(ts any, res []service.IColPoolRes) (int, []service.IColPoolRes, error) {
			metricData, ok := ts.(*model.TimeSamplesData)
			if !ok {
				logger.Info("invalid request  metric")
				return 0, nil, fmt.Errorf("invalid request  metric")
			}
			metrics := (&MetricsAcquirer{}).deserialize(res)
			_len := len(metrics.Fingerprint.Data)
			for _, tn := range metricData.MType {
				metrics.Type.Data.Append(tn)
			}
			for _, tn := range metricData.MTimestampNS {
				metrics.TimestampNS.Data.Append(tn)
			}

			for _, tn := range metricData.MFingerprint {
				metrics.Fingerprint.Data.Append(tn)
			}
			for _, tn := range metricData.MValue {
				metrics.Value.Data.Append(tn)
			}

			return len(metrics.Fingerprint.Data) - _len, res, nil
		},
	}
}
