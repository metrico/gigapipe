package insert

import (
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/metrico/qryn/writer/model"
	"github.com/metrico/qryn/writer/plugins"
	"github.com/metrico/qryn/writer/service"
	"github.com/metrico/qryn/writer/utils/logger"
)

type SamplesAcquirer struct {
	Type        *service.PooledColumn[proto.ColUInt8]
	Fingerprint *service.PooledColumn[proto.ColUInt64]
	TimestampNS *service.PooledColumn[proto.ColInt64]
	String      *service.PooledColumn[*proto.ColStr]
	Value       *service.PooledColumn[proto.ColFloat64]
}

func (a *SamplesAcquirer) acq() *SamplesAcquirer {
	service.StartAcq()
	defer service.FinishAcq()
	a.Type = service.UInt8Pool.Acquire("type")
	a.Fingerprint = service.UInt64Pool.Acquire("fingerprint")
	a.TimestampNS = service.Int64Pool.Acquire("timestamp_ns")
	a.String = service.StrPool.Acquire("string")
	a.Value = service.Float64Pool.Acquire("value")
	return a
}

func (a *SamplesAcquirer) serialize() []service.IColPoolRes {
	return []service.IColPoolRes{a.Type, a.Fingerprint, a.TimestampNS, a.String, a.Value}
}

func (a *SamplesAcquirer) deserialize(res []service.IColPoolRes) *SamplesAcquirer {
	a.Type, a.Fingerprint, a.TimestampNS, a.String, a.Value =

		res[0].(*service.PooledColumn[proto.ColUInt8]),
		res[1].(*service.PooledColumn[proto.ColUInt64]),
		res[2].(*service.PooledColumn[proto.ColInt64]),
		res[3].(*service.PooledColumn[*proto.ColStr]),
		res[4].(*service.PooledColumn[proto.ColFloat64])
	return a
}

func NewSamplesInsertService(opts model.InsertServiceOpts) service.IInsertServiceV2 {

	plugin := plugins.GetSamplesInsertServicePlugin()
	if plugin != nil {
		return (*plugin)(opts)
	}
	if opts.ParallelNum <= 0 {
		opts.ParallelNum = 1
	}
	table := "samples_v3"
	if opts.Node.ClusterName != "" {
		table += "_dist"
	}
	insertReq := fmt.Sprintf("INSERT INTO %s (type,fingerprint, timestamp_ns, string, value)",
		table)
	return &service.InsertServiceV2Multimodal{
		ServiceData:    service.ServiceData{},
		V3Session:      opts.Session,
		DatabaseNode:   opts.Node,
		PushInterval:   opts.Interval,
		SvcNum:         opts.ParallelNum,
		AsyncInsert:    opts.AsyncInsert,
		MaxQueueSize:   opts.MaxQueueSize,
		OnBeforeInsert: opts.OnBeforeInsert,
		InsertRequest:  insertReq,
		ServiceType:    "samples",
		AcquireColumns: func() []service.IColPoolRes {
			return (&SamplesAcquirer{}).acq().serialize()
		},
		ProcessRequest: func(ts any, res []service.IColPoolRes) (int, []service.IColPoolRes, error) {
			timeSeriesData, ok := ts.(*model.TimeSamplesData)
			if !ok {
				logger.Info("NewSamplesInsertService")
				return 0, nil, fmt.Errorf("invalid request samples insert")
			}
			samples := (&SamplesAcquirer{}).deserialize(res)
			_len := len(samples.Fingerprint.Data)

			for _, timeNs := range timeSeriesData.MTimestampNS {
				samples.TimestampNS.Data.Append(timeNs)
			}

			for _, mf := range timeSeriesData.MFingerprint {
				samples.Fingerprint.Data.Append(mf)
			}
			for _, mt := range timeSeriesData.MType {
				samples.Type.Data.Append(mt)
			}

			for _, mValue := range timeSeriesData.MValue {
				samples.Value.Data.Append(mValue)
			}
			for _, mMessage := range timeSeriesData.MMessage {
				samples.String.Data.Append(mMessage)
			}
			return len(samples.Fingerprint.Data) - _len, samples.serialize(), nil
		},
	}
}
