package insert

import (
	"fmt"
	"strings"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/service"
)

// newMetricInsertService builds a metric insert service over table, whose
// insert columns are named by columns and filled by fill.
func newMetricInsertService[T any](opts model.InsertServiceOpts, table string, serviceType string,
	columns []string, acquire func() []service.IColPoolRes, fill func(req T, cols []service.IColPoolRes),
) service.IInsertServiceV2 {
	if opts.ParallelNum <= 0 {
		opts.ParallelNum = 1
	}
	if opts.Node.ClusterName != "" {
		table += "_dist"
	}
	return &service.InsertServiceV2Multimodal{
		ServiceData:    service.ServiceData{},
		V3Session:      opts.Session,
		DatabaseNode:   opts.Node,
		PushInterval:   opts.Interval,
		SvcNum:         opts.ParallelNum,
		AsyncInsert:    opts.AsyncInsert,
		MaxQueueSize:   opts.MaxQueueSize,
		OnBeforeInsert: opts.OnBeforeInsert,
		InsertRequest:  fmt.Sprintf("INSERT INTO %s (%s)", table, strings.Join(columns, ", ")),
		ServiceType:    serviceType,
		AcquireColumns: acquire,
		ProcessRequest: func(req any, cols []service.IColPoolRes) (int, []service.IColPoolRes, error) {
			data, ok := req.(T)
			if !ok {
				return 0, nil, fmt.Errorf("invalid request for %s", table)
			}
			before := cols[0].Input().Data.Rows()
			fill(data, cols)
			return cols[0].Input().Data.Rows() - before, cols, nil
		},
	}
}

func appendMs(col *proto.ColDateTime64, ms []int64) {
	for _, v := range ms {
		col.AppendRaw(proto.DateTime64(v))
	}
}

func NewMetricStagingInsertService(opts model.InsertServiceOpts) service.IInsertServiceV2 {
	return newMetricInsertService(opts, "metric_samples_in", "metric_samples",
		[]string{"fingerprint", "timestamp", "value", "prev_timestamp", "prev_value", "aggregate"},
		func() []service.IColPoolRes {
			service.StartAcq()
			defer service.FinishAcq()
			return []service.IColPoolRes{
				service.UInt64Pool.Acquire("fingerprint"),
				service.DateTime64MsPool.Acquire("timestamp"),
				service.Float64Pool.Acquire("value"),
				service.DateTime64MsPool.Acquire("prev_timestamp"),
				service.Float64Pool.Acquire("prev_value"),
				service.UInt8Pool.Acquire("aggregate"),
			}
		},
		func(d *model.MetricSamplesData, cols []service.IColPoolRes) {
			fp := cols[0].(*service.PooledColumn[proto.ColUInt64])
			value := cols[2].(*service.PooledColumn[proto.ColFloat64])
			prevValue := cols[4].(*service.PooledColumn[proto.ColFloat64])
			aggregate := cols[5].(*service.PooledColumn[proto.ColUInt8])
			fp.Data = append(fp.Data, d.MFingerprint...)
			appendMs(cols[1].(*service.PooledColumn[*proto.ColDateTime64]).Data, d.MTimestampMs)
			value.Data = append(value.Data, d.MValue...)
			appendMs(cols[3].(*service.PooledColumn[*proto.ColDateTime64]).Data, d.MPrevTimestampMs)
			prevValue.Data = append(prevValue.Data, d.MPrevValue...)
			aggregate.Data = append(aggregate.Data, d.MAggregate...)
		})
}

func NewMetricSeriesInsertService(opts model.InsertServiceOpts) service.IInsertServiceV2 {
	return newMetricInsertService(opts, "metric_series", "metric_series",
		[]string{"name", "fingerprint", "labels", "first_seen", "last_seen"},
		func() []service.IColPoolRes {
			service.StartAcq()
			defer service.FinishAcq()
			return []service.IColPoolRes{
				service.LowCardinalityStrPool.Acquire("name"),
				service.UInt64Pool.Acquire("fingerprint"),
				service.LabelsMapPool.Acquire("labels"),
				service.DateTime64MsPool.Acquire("first_seen"),
				service.DateTime64MsPool.Acquire("last_seen"),
			}
		},
		func(d *model.MetricSeriesData, cols []service.IColPoolRes) {
			cols[0].(*service.PooledColumn[*proto.ColLowCardinality[string]]).Data.AppendArr(d.MName)
			fp := cols[1].(*service.PooledColumn[proto.ColUInt64])
			fp.Data = append(fp.Data, d.MFingerprint...)
			cols[2].(*service.PooledColumn[*proto.ColMap[string, string]]).Data.AppendArr(d.MLabels)
			appendMs(cols[3].(*service.PooledColumn[*proto.ColDateTime64]).Data, d.MFirstSeenMs)
			appendMs(cols[4].(*service.PooledColumn[*proto.ColDateTime64]).Data, d.MLastSeenMs)
		})
}

func NewMetricMetadataInsertService(opts model.InsertServiceOpts) service.IInsertServiceV2 {
	return newMetricInsertService(opts, "metric_metadata", "metric_metadata",
		[]string{"name", "type", "help", "unit", "updated_at"},
		func() []service.IColPoolRes {
			service.StartAcq()
			defer service.FinishAcq()
			return []service.IColPoolRes{
				service.LowCardinalityStrPool.Acquire("name"),
				service.LowCardinalityStrPool.Acquire("type"),
				service.StrPool.Acquire("help"),
				service.LowCardinalityStrPool.Acquire("unit"),
				service.DateTime64MsPool.Acquire("updated_at"),
			}
		},
		func(d *model.MetricMetadataData, cols []service.IColPoolRes) {
			cols[0].(*service.PooledColumn[*proto.ColLowCardinality[string]]).Data.AppendArr(d.MName)
			cols[1].(*service.PooledColumn[*proto.ColLowCardinality[string]]).Data.AppendArr(d.MType)
			cols[2].(*service.PooledColumn[*proto.ColStr]).Data.AppendArr(d.MHelp)
			cols[3].(*service.PooledColumn[*proto.ColLowCardinality[string]]).Data.AppendArr(d.MUnit)
			appendMs(cols[4].(*service.PooledColumn[*proto.ColDateTime64]).Data, d.MUpdatedAtMs)
		})
}

func NewMetricExemplarsInsertService(opts model.InsertServiceOpts) service.IInsertServiceV2 {
	return newMetricInsertService(opts, "metric_exemplars", "metric_exemplars",
		[]string{"fingerprint", "timestamp", "value", "trace_id", "labels"},
		func() []service.IColPoolRes {
			service.StartAcq()
			defer service.FinishAcq()
			return []service.IColPoolRes{
				service.UInt64Pool.Acquire("fingerprint"),
				service.DateTime64MsPool.Acquire("timestamp"),
				service.Float64Pool.Acquire("value"),
				service.StrPool.Acquire("trace_id"),
				service.StrPool.Acquire("labels"),
			}
		},
		func(d *model.MetricExemplarsData, cols []service.IColPoolRes) {
			fp := cols[0].(*service.PooledColumn[proto.ColUInt64])
			fp.Data = append(fp.Data, d.MFingerprint...)
			appendMs(cols[1].(*service.PooledColumn[*proto.ColDateTime64]).Data, d.MTimestampMs)
			value := cols[2].(*service.PooledColumn[proto.ColFloat64])
			value.Data = append(value.Data, d.MValue...)
			cols[3].(*service.PooledColumn[*proto.ColStr]).Data.AppendArr(d.MTraceID)
			cols[4].(*service.PooledColumn[*proto.ColStr]).Data.AppendArr(d.MLabels)
		})
}
