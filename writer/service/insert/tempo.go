package insert

import (
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/metrico/qryn/v4/writer/model"
	"github.com/metrico/qryn/v4/writer/plugins"
	"github.com/metrico/qryn/v4/writer/service"
	"github.com/metrico/qryn/v4/writer/utils/logger"
)

type tempoSamplesAcquirer struct {
	traceId     *service.PooledColumn[*proto.ColFixedStr]
	spanId      *service.PooledColumn[*proto.ColFixedStr]
	parentId    *service.PooledColumn[*proto.ColStr]
	name        *service.PooledColumn[*proto.ColStr]
	timestampNs *service.PooledColumn[proto.ColInt64]
	durationNs  *service.PooledColumn[proto.ColInt64]
	serviceName *service.PooledColumn[*proto.ColStr]
	payloadType *service.PooledColumn[proto.ColInt8]
	payload     *service.PooledColumn[*proto.ColStr]
}

func (t *tempoSamplesAcquirer) acq() *tempoSamplesAcquirer {
	service.StartAcq()
	defer service.FinishAcq()

	t.traceId = service.FixedStringPool.Acquire("trace_id")
	t.traceId.Data.SetSize(16)
	t.spanId = service.FixedStringPool.Acquire("span_id")
	t.spanId.Data.SetSize(8)
	t.parentId = service.StrPool.Acquire("parent_id")
	t.name = service.StrPool.Acquire("name")
	t.timestampNs = service.Int64Pool.Acquire("timestamp_ns")
	t.durationNs = service.Int64Pool.Acquire("duration_ns")
	t.serviceName = service.StrPool.Acquire("service_name")
	t.payloadType = service.Int8ColPool.Acquire("payload_type")
	t.payload = service.StrPool.Acquire("payload")
	return t
}

func (t *tempoSamplesAcquirer) toIFace() []service.IColPoolRes {
	return []service.IColPoolRes{
		t.traceId,
		t.spanId,
		t.parentId,
		t.name,
		t.timestampNs,
		t.durationNs,
		t.serviceName,
		t.payloadType,
		t.payload,
	}
}

func (t *tempoSamplesAcquirer) fromIFace(iface []service.IColPoolRes) *tempoSamplesAcquirer {
	t.traceId = iface[0].(*service.PooledColumn[*proto.ColFixedStr])
	t.spanId = iface[1].(*service.PooledColumn[*proto.ColFixedStr])
	t.parentId = iface[2].(*service.PooledColumn[*proto.ColStr])
	t.name = iface[3].(*service.PooledColumn[*proto.ColStr])
	t.timestampNs = iface[4].(*service.PooledColumn[proto.ColInt64])
	t.durationNs = iface[5].(*service.PooledColumn[proto.ColInt64])
	t.serviceName = iface[6].(*service.PooledColumn[*proto.ColStr])
	t.payloadType = iface[7].(*service.PooledColumn[proto.ColInt8])
	t.payload = iface[8].(*service.PooledColumn[*proto.ColStr])
	return t
}

type BoolWrap struct {
	bc *proto.ColBool
}

func (b *BoolWrap) Append(v bool) {
	*b.bc = append(*b.bc, v)
}

func NewTempoSamplesInsertService(opts model.InsertServiceOpts) service.IInsertServiceV2 {
	plugin := plugins.GetTracesInsertServicePlugin()
	if plugin != nil {
		return (*plugin)(opts)
	}
	if opts.ParallelNum <= 0 {
		opts.ParallelNum = 1
	}
	tableName := "tempo_traces"
	if opts.Node.ClusterName != "" {
		tableName += "_dist"
	}
	insertRequest := fmt.Sprintf("INSERT INTO %s "+
		"(trace_id ,span_id, parent_id, name, timestamp_ns, "+
		"duration_ns, service_name, payload_type, payload)", tableName)
	return &service.InsertServiceV2Multimodal{
		ServiceData:    service.ServiceData{},
		V3Session:      opts.Session,
		DatabaseNode:   opts.Node,
		PushInterval:   opts.Interval,
		InsertRequest:  insertRequest,
		SvcNum:         opts.ParallelNum,
		AsyncInsert:    opts.AsyncInsert,
		MaxQueueSize:   opts.MaxQueueSize,
		OnBeforeInsert: opts.OnBeforeInsert,
		ServiceType:    "traces",
		AcquireColumns: func() []service.IColPoolRes {
			return (&tempoSamplesAcquirer{}).acq().toIFace()
		},
		ProcessRequest: func(v2 any, res []service.IColPoolRes) (int, []service.IColPoolRes, error) {
			tempSamples, ok := v2.(*model.TempoSamples)
			if !ok {
				logger.Info("invalid request type tempo samples")
				return 0, nil, fmt.Errorf("invalid request type tempo samples")
			}
			acquirer := (&tempoSamplesAcquirer{}).fromIFace(res)
			s1 := res[0].Size()
			(&service.FixedStrAdaptor{ColFixedStr: acquirer.traceId.Data}).AppendArr(tempSamples.MTraceId)
			(&service.FixedStrAdaptor{ColFixedStr: acquirer.spanId.Data}).AppendArr(tempSamples.MSpanId)
			(&service.Int64Adaptor{ColInt64: &acquirer.timestampNs.Data}).AppendArr(tempSamples.MTimestampNs)
			(&service.Int64Adaptor{ColInt64: &acquirer.durationNs.Data}).AppendArr(tempSamples.MDurationNs)
			acquirer.name.Data.AppendArr(tempSamples.MName)
			acquirer.parentId.Data.AppendArr(tempSamples.MParentId)
			for _, p := range tempSamples.MPayload {
				acquirer.payload.Data.AppendBytes(p)
			}
			(&service.I8Adaptor{ColInt8: &acquirer.payloadType.Data}).AppendArr(tempSamples.MPayloadType)
			acquirer.serviceName.Data.AppendArr(tempSamples.MServiceName)
			return res[0].Size() - s1, acquirer.toIFace(), nil
		},
	}
}

type tempoTagsAcquirer struct {
	date        *service.PooledColumn[proto.ColDate]
	key         *service.PooledColumn[*proto.ColStr]
	val         *service.PooledColumn[*proto.ColStr]
	traceId     *service.PooledColumn[*proto.ColFixedStr]
	spanId      *service.PooledColumn[*proto.ColFixedStr]
	timestampNS *service.PooledColumn[proto.ColInt64]
	durationNS  *service.PooledColumn[proto.ColInt64]
}

func (t *tempoTagsAcquirer) acq() *tempoTagsAcquirer {
	service.StartAcq()
	defer service.FinishAcq()
	t.date = service.DatePool.Acquire("date")
	t.key = service.StrPool.Acquire("key")
	t.val = service.StrPool.Acquire("val")
	t.traceId = service.FixedStringPool.Acquire("trace_id")
	t.traceId.Data.SetSize(16)
	t.spanId = service.FixedStringPool.Acquire("span_id")
	t.spanId.Data.SetSize(8)
	t.timestampNS = service.Int64Pool.Acquire("timestamp_ns")
	t.timestampNS.Data.Reset()
	t.durationNS = service.Int64Pool.Acquire("duration")
	t.durationNS.Data.Reset()
	return t
}

func (t *tempoTagsAcquirer) toIFace() []service.IColPoolRes {
	return []service.IColPoolRes{
		t.date,
		t.key,
		t.val,
		t.traceId,
		t.spanId,
		t.timestampNS,
		t.durationNS,
	}
}

func (t *tempoTagsAcquirer) fromIFace(iface []service.IColPoolRes) *tempoTagsAcquirer {
	t.date = iface[0].(*service.PooledColumn[proto.ColDate])
	t.key = iface[1].(*service.PooledColumn[*proto.ColStr])
	t.val = iface[2].(*service.PooledColumn[*proto.ColStr])
	t.traceId = iface[3].(*service.PooledColumn[*proto.ColFixedStr])
	t.spanId = iface[4].(*service.PooledColumn[*proto.ColFixedStr])
	t.timestampNS = iface[5].(*service.PooledColumn[proto.ColInt64])
	t.durationNS = iface[6].(*service.PooledColumn[proto.ColInt64])
	return t
}

func NewTempoTagsInsertService(opts model.InsertServiceOpts) service.IInsertServiceV2 {
	if opts.ParallelNum <= 0 {
		opts.ParallelNum = 1
	}
	tableName := "tempo_traces_attrs_gin"
	if opts.Node.ClusterName != "" {
		tableName += "_dist"
	}
	insertRequest := fmt.Sprintf("INSERT INTO %s (date, key, val, trace_id, span_id, timestamp_ns, duration)",
		tableName)
	return &service.InsertServiceV2Multimodal{
		ServiceData:    service.ServiceData{},
		V3Session:      opts.Session,
		DatabaseNode:   opts.Node,
		PushInterval:   opts.Interval,
		InsertRequest:  insertRequest,
		SvcNum:         opts.ParallelNum,
		AsyncInsert:    opts.AsyncInsert,
		MaxQueueSize:   opts.MaxQueueSize,
		OnBeforeInsert: opts.OnBeforeInsert,
		ServiceType:    "traces_tags",

		AcquireColumns: func() []service.IColPoolRes {
			return (&tempoTagsAcquirer{}).acq().toIFace()
		},
		ProcessRequest: func(v2 any, res []service.IColPoolRes) (int, []service.IColPoolRes, error) {
			tempTags, ok := v2.(*model.TempoTag)
			if !ok {
				return 0, nil, fmt.Errorf("invalid request tempo tags")
			}

			acquirer := (&tempoTagsAcquirer{}).fromIFace(res)
			s1 := res[0].Size()
			(&service.FixedStrAdaptor{ColFixedStr: acquirer.traceId.Data}).AppendArr(tempTags.MTraceId)
			(&service.FixedStrAdaptor{ColFixedStr: acquirer.spanId.Data}).AppendArr(tempTags.MSpanId)
			(&service.Int64Adaptor{ColInt64: &acquirer.timestampNS.Data}).AppendArr(tempTags.MTimestampNs)
			(&service.Int64Adaptor{ColInt64: &acquirer.durationNS.Data}).AppendArr(tempTags.MDurationNs)
			acquirer.key.Data.AppendArr(tempTags.MKey)
			acquirer.val.Data.AppendArr(tempTags.MVal)
			(&service.DateAppender{D: &acquirer.date.Data}).AppendArr(tempTags.MDate)
			return res[0].Size() - s1, acquirer.toIFace(), nil
		},
	}
}
