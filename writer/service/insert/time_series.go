package insert

import (
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/metrico/qryn/v4/writer/model"
	"github.com/metrico/qryn/v4/writer/plugins"
	"github.com/metrico/qryn/v4/writer/service"
	"github.com/metrico/qryn/v4/writer/utils/logger"
)

type TimeSeriesAcquirer struct {
	Type        *service.PooledColumn[proto.ColUInt8]
	Date        *service.PooledColumn[proto.ColDate]
	Fingerprint *service.PooledColumn[proto.ColUInt64]
	Labels      *service.PooledColumn[*proto.ColStr]
}

func (a *TimeSeriesAcquirer) acq() *TimeSeriesAcquirer {
	service.StartAcq()
	defer service.FinishAcq()
	a.Type = service.UInt8Pool.Acquire("type")
	a.Date = service.DatePool.Acquire("date")
	a.Fingerprint = service.UInt64Pool.Acquire("fingerprint")
	a.Labels = service.StrPool.Acquire("labels")
	return a
}

func (a *TimeSeriesAcquirer) serialize() []service.IColPoolRes {
	return []service.IColPoolRes{a.Type, a.Date, a.Fingerprint, a.Labels}
}

func (a *TimeSeriesAcquirer) deserialize(res []service.IColPoolRes) *TimeSeriesAcquirer {
	a.Type, a.Date, a.Fingerprint, a.Labels =
		res[0].(*service.PooledColumn[proto.ColUInt8]),
		res[1].(*service.PooledColumn[proto.ColDate]),
		res[2].(*service.PooledColumn[proto.ColUInt64]),
		res[3].(*service.PooledColumn[*proto.ColStr])
	return a
}

func NewTimeSeriesInsertService(opts model.InsertServiceOpts) service.IInsertServiceV2 {

	plugin := plugins.GetTimeSeriesInsertServicePlugin()
	if plugin != nil {
		return (*plugin)(opts)
	}
	if opts.ParallelNum <= 0 {
		opts.ParallelNum = 1
	}
	table := "time_series"
	if opts.Node.ClusterName != "" {
		table += "_dist"
	}
	insertReq := fmt.Sprintf("INSERT INTO %s (type, date, fingerprint, labels)",
		table)
	return &service.InsertServiceV2Multimodal{
		ServiceData:    service.ServiceData{},
		V3Session:      opts.Session,
		DatabaseNode:   opts.Node,
		PushInterval:   opts.Interval,
		MaxQueueSize:   opts.MaxQueueSize,
		OnBeforeInsert: opts.OnBeforeInsert,
		SvcNum:         opts.ParallelNum,
		AsyncInsert:    opts.AsyncInsert,
		InsertRequest:  insertReq,
		ServiceType:    "time_series",
		AcquireColumns: func() []service.IColPoolRes {
			return (&TimeSeriesAcquirer{}).acq().serialize()
		},
		ProcessRequest: func(ts any, res []service.IColPoolRes) (int, []service.IColPoolRes, error) {
			timeSeriesData, ok := ts.(*model.TimeSeriesData)
			if !ok {
				logger.Info("invalid request type time series")
				return 0, nil, fmt.Errorf("invalid request type time series")
			}
			acquirer := (&TimeSeriesAcquirer{}).deserialize(res)
			_len := len(acquirer.Date.Data)

			for i, d := range timeSeriesData.MDate {
				acquirer.Date.Data.Append(d)
				acquirer.Labels.Data.Append(timeSeriesData.MLabels[i])
			}

			for _, Mf := range timeSeriesData.MFingerprint {
				acquirer.Fingerprint.Data.Append(Mf)
			}

			for _, MT := range timeSeriesData.MType {
				acquirer.Type.Data.Append(MT)
			}
			return len(acquirer.Date.Data) - _len, acquirer.serialize(), nil
		},
	}
}
