package insert

import (
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/metrico/qryn/v5/shared/federation"
	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/plugins"
	"github.com/metrico/qryn/v5/writer/service"
	"github.com/metrico/qryn/v5/writer/utils/logger"
)

type TimeSeriesAcquirer struct {
	Type        *service.PooledColumn[proto.ColUInt8]
	Date        *service.PooledColumn[proto.ColDate]
	Fingerprint *service.PooledColumn[proto.ColUInt64]
	Labels      *service.PooledColumn[*proto.ColStr]
	Metadata    *service.PooledColumn[*proto.ColStr]
	// Oid is populated only when federation is enabled; maps to the leading
	// `oid` column of time_series. See shared/federation.
	Oid *service.PooledColumn[*proto.ColStr]
}

func (a *TimeSeriesAcquirer) acq() *TimeSeriesAcquirer {
	service.StartAcq()
	defer service.FinishAcq()
	if federation.Enabled() {
		a.Oid = service.StrPool.Acquire("oid")
	}
	a.Type = service.UInt8Pool.Acquire("type")
	a.Date = service.DatePool.Acquire("date")
	a.Fingerprint = service.UInt64Pool.Acquire("fingerprint")
	a.Labels = service.StrPool.Acquire("labels")
	a.Metadata = service.StrPool.Acquire("metadata")
	return a
}

func (a *TimeSeriesAcquirer) serialize() []service.IColPoolRes {
	if federation.Enabled() {
		return []service.IColPoolRes{a.Oid, a.Type, a.Date, a.Fingerprint, a.Labels, a.Metadata}
	}
	return []service.IColPoolRes{a.Type, a.Date, a.Fingerprint, a.Labels, a.Metadata}
}

func (a *TimeSeriesAcquirer) deserialize(res []service.IColPoolRes) *TimeSeriesAcquirer {
	if federation.Enabled() {
		a.Oid = res[0].(*service.PooledColumn[*proto.ColStr])
		res = res[1:]
	}
	a.Type, a.Date, a.Fingerprint, a.Labels, a.Metadata = res[0].(*service.PooledColumn[proto.ColUInt8]),
		res[1].(*service.PooledColumn[proto.ColDate]),
		res[2].(*service.PooledColumn[proto.ColUInt64]),
		res[3].(*service.PooledColumn[*proto.ColStr]),
		res[4].(*service.PooledColumn[*proto.ColStr])
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
	cols := "type, date, fingerprint, labels, metadata"
	if federation.Enabled() {
		cols = "oid, " + cols
	}
	insertReq := fmt.Sprintf("INSERT INTO %s (%s)", table, cols)
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

			if federation.Enabled() {
				for range timeSeriesData.MDate {
					acquirer.Oid.Data.Append(timeSeriesData.MOid)
				}
			}

			for i, d := range timeSeriesData.MDate {
				acquirer.Date.Data.Append(d)
				acquirer.Labels.Data.Append(timeSeriesData.MLabels[i])
				// Append metadata if available, otherwise empty string
				if i < len(timeSeriesData.MMetadata) {
					acquirer.Metadata.Data.Append(timeSeriesData.MMetadata[i])
				} else {
					acquirer.Metadata.Data.Append("")
				}
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
