package insert

import (
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/metrico/qryn/v4/logger"
	"github.com/metrico/qryn/v4/writer/model"
	"github.com/metrico/qryn/v4/writer/plugins"
	"github.com/metrico/qryn/v4/writer/service"
)

type profileSamplesAcquirer struct {
	timestampNs      *service.PooledColumn[proto.ColUInt64]
	ptype            *service.PooledColumn[*proto.ColStr]
	serviceName      *service.PooledColumn[*proto.ColStr]
	sampleTypesUnits *service.PooledColumn[*proto.ColArr[model.StrStr]]
	periodType       *service.PooledColumn[*proto.ColStr]
	periodUnit       *service.PooledColumn[*proto.ColStr]
	tags             *service.PooledColumn[*proto.ColArr[model.StrStr]]
	durationNs       *service.PooledColumn[proto.ColUInt64]
	payloadType      *service.PooledColumn[*proto.ColStr]
	payload          *service.PooledColumn[*proto.ColStr]
	valuesAgg        *service.PooledColumn[*proto.ColArr[model.ValuesAgg]]
	tree             *service.PooledColumn[*proto.ColArr[model.TreeRootStructure]]
	functions        *service.PooledColumn[*proto.ColArr[model.Function]]
}

func (t *profileSamplesAcquirer) acq() *profileSamplesAcquirer {
	service.StartAcq()
	defer service.FinishAcq()
	t.timestampNs = service.UInt64Pool.Acquire("timestamp_ns")
	t.ptype = service.StrPool.Acquire("type")
	t.serviceName = service.StrPool.Acquire("service_name")
	t.sampleTypesUnits = service.TupleStrStrPool.Acquire("sample_types_units")
	t.periodType = service.StrPool.Acquire("period_type")
	t.periodUnit = service.StrPool.Acquire("period_unit")
	t.tags = service.TupleStrStrPool.Acquire("tags")
	t.durationNs = service.UInt64Pool.Acquire("duration_ns")
	t.payloadType = service.StrPool.Acquire("payload_type")
	t.payload = service.StrPool.Acquire("payload")
	t.valuesAgg = service.TupleStrInt64Int32Pool.Acquire("values_agg")
	t.tree = service.TupleUInt64UInt64UInt64ArrPool.Acquire("tree")
	t.functions = service.TupleUInt64StrPool.Acquire("functions")

	return t
}

func (t *profileSamplesAcquirer) toIFace() []service.IColPoolRes {
	return []service.IColPoolRes{
		t.timestampNs,
		t.ptype,
		t.serviceName,
		t.sampleTypesUnits,
		t.periodType,
		t.periodUnit,
		t.tags,
		t.durationNs,
		t.payloadType,
		t.payload,
		t.valuesAgg,
		t.tree,
		t.functions,
	}
}

func (t *profileSamplesAcquirer) fromIFace(iface []service.IColPoolRes) *profileSamplesAcquirer {
	t.timestampNs = iface[0].(*service.PooledColumn[proto.ColUInt64])
	t.ptype = iface[1].(*service.PooledColumn[*proto.ColStr])
	t.serviceName = iface[2].(*service.PooledColumn[*proto.ColStr])
	t.sampleTypesUnits = iface[3].(*service.PooledColumn[*proto.ColArr[model.StrStr]])
	t.periodType = iface[4].(*service.PooledColumn[*proto.ColStr])
	t.periodUnit = iface[5].(*service.PooledColumn[*proto.ColStr])
	t.tags = iface[6].(*service.PooledColumn[*proto.ColArr[model.StrStr]])
	t.durationNs = iface[7].(*service.PooledColumn[proto.ColUInt64])
	t.payloadType = iface[8].(*service.PooledColumn[*proto.ColStr])
	t.payload = iface[9].(*service.PooledColumn[*proto.ColStr])
	t.valuesAgg = iface[10].(*service.PooledColumn[*proto.ColArr[model.ValuesAgg]])
	t.tree = iface[11].(*service.PooledColumn[*proto.ColArr[model.TreeRootStructure]])
	t.functions = iface[12].(*service.PooledColumn[*proto.ColArr[model.Function]])
	return t
}

func NewProfileSamplesInsertService(opts model.InsertServiceOpts) service.IInsertServiceV2 {
	plugin := plugins.GetProfileInsertServicePlugin()
	if plugin != nil {
		return (*plugin)(opts)
	}
	if opts.ParallelNum <= 0 {
		opts.ParallelNum = 1
	}
	tableName := "profiles_input"
	insertRequest := fmt.Sprintf("INSERT INTO %s "+
		"(timestamp_ns, type, service_name, sample_types_units, period_type, period_unit,tags, duration_ns, payload_type, payload, values_agg,tree,functions)", tableName)
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
		ServiceType:    "profile",
		AcquireColumns: func() []service.IColPoolRes {
			return (&profileSamplesAcquirer{}).acq().toIFace()
		},
		ProcessRequest: func(ts any, res []service.IColPoolRes) (int, []service.IColPoolRes, error) {
			profileSeriesData, ok := ts.(*model.ProfileData)
			if !ok {
				logger.Info("profileSeriesData")
				return 0, nil, fmt.Errorf("invalid request samples insert")
			}
			acquirer := (&profileSamplesAcquirer{}).fromIFace(res)
			// snap := acquirer.snapshot()
			s1 := res[0].Size()

			(&service.Uint64Adaptor{ColUInt64: &acquirer.timestampNs.Data}).AppendArr(profileSeriesData.TimestampNs)
			(&service.Uint64Adaptor{ColUInt64: &acquirer.durationNs.Data}).AppendArr(profileSeriesData.DurationNs)
			for _, serviceName := range profileSeriesData.ServiceName {
				acquirer.serviceName.Data.Append(serviceName)
			}

			for _, pt := range profileSeriesData.Ptype {
				acquirer.ptype.Data.Append(pt)
			}

			for _, payloadType := range profileSeriesData.PayloadType {
				acquirer.payloadType.Data.Append(payloadType)
			}

			for _, periodUnit := range profileSeriesData.PeriodUnit {
				acquirer.periodUnit.Data.Append(periodUnit)
			}

			for _, periodType := range profileSeriesData.PeriodType {
				acquirer.periodType.Data.Append(periodType)
			}
			for _, payload := range profileSeriesData.Payload {
				acquirer.payload.Data.AppendBytes(payload)
			}

			acquirer.sampleTypesUnits.Data.Append(profileSeriesData.SamplesTypesUnits)
			acquirer.tags.Data.Append(profileSeriesData.Tags)
			acquirer.valuesAgg.Data.Append(profileSeriesData.ValuesAgg)
			acquirer.functions.Data.Append(profileSeriesData.Function)
			acquirer.tree.Data.Append(profileSeriesData.Tree)

			return res[0].Size() - s1, acquirer.toIFace(), nil
		},
	}
}
