package insert

import (
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/metrico/qryn/v4/writer/model"
	"github.com/metrico/qryn/v4/writer/service"
)

type patternAcquirer struct {
	timestamp10m    *service.PooledColumn[proto.ColUInt32]
	fingerprint     *service.PooledColumn[proto.ColUInt64]
	timestampS      *service.PooledColumn[proto.ColUInt32]
	tokens          *service.PooledColumn[*proto.ColArr[string]]
	classes         *service.PooledColumn[*proto.ColArr[uint32]]
	overallCost     *service.PooledColumn[proto.ColUInt32]
	generalizedCost *service.PooledColumn[proto.ColUInt32]
	samplesCount    *service.PooledColumn[proto.ColUInt32]
	patternId       *service.PooledColumn[proto.ColUInt64]
	iterationId     *service.PooledColumn[proto.ColUInt64]
}

func (t *patternAcquirer) acq() *patternAcquirer {
	service.StartAcq()
	defer service.FinishAcq()
	t.timestamp10m = service.UInt32Pool.Acquire("timestamp_10m")
	t.fingerprint = service.UInt64Pool.Acquire("fingerprint")
	t.timestampS = service.UInt32Pool.Acquire("timestamp_s")
	t.tokens = service.StrArrayPool.Acquire("tokens")
	t.classes = service.UInt32ArrayPool.Acquire("classes")
	t.overallCost = service.UInt32Pool.Acquire("overall_cost")
	t.generalizedCost = service.UInt32Pool.Acquire("generalized_cost")
	t.samplesCount = service.UInt32Pool.Acquire("samples_count")
	t.patternId = service.UInt64Pool.Acquire("pattern_id")
	t.iterationId = service.UInt64Pool.Acquire("iteration_id")
	return t
}

func (t *patternAcquirer) toIFace() []service.IColPoolRes {
	return []service.IColPoolRes{
		t.timestamp10m,
		t.fingerprint,
		t.timestampS,
		t.tokens,
		t.classes,
		t.overallCost,
		t.generalizedCost,
		t.samplesCount,
		t.patternId,
		t.iterationId,
	}
}

func (t *patternAcquirer) fromIFace(iface []service.IColPoolRes) *patternAcquirer {
	t.timestamp10m = iface[0].(*service.PooledColumn[proto.ColUInt32])
	t.fingerprint = iface[1].(*service.PooledColumn[proto.ColUInt64])
	t.timestampS = iface[2].(*service.PooledColumn[proto.ColUInt32])
	t.tokens = iface[3].(*service.PooledColumn[*proto.ColArr[string]])
	t.classes = iface[4].(*service.PooledColumn[*proto.ColArr[uint32]])
	t.overallCost = iface[5].(*service.PooledColumn[proto.ColUInt32])
	t.generalizedCost = iface[6].(*service.PooledColumn[proto.ColUInt32])
	t.samplesCount = iface[7].(*service.PooledColumn[proto.ColUInt32])
	t.patternId = iface[8].(*service.PooledColumn[proto.ColUInt64])
	t.iterationId = iface[9].(*service.PooledColumn[proto.ColUInt64])
	return t
}

func NewPatternInsertService(opts model.InsertServiceOpts) service.IInsertServiceV2 {
	if opts.ParallelNum <= 0 {
		opts.ParallelNum = 1
	}
	tableName := "patterns"
	if opts.Node.ClusterName != "" {
		tableName += "_dist"
	}
	insertReq := fmt.Sprintf("INSERT INTO %s (timestamp_10m, fingerprint, timestamp_s, tokens, classes, overall_cost, generalized_cost, samples_count, pattern_id, iteration_id)",
		tableName)
	return &service.InsertServiceV2Multimodal{
		ServiceData:   service.ServiceData{},
		V3Session:     opts.Session,
		DatabaseNode:  opts.Node,
		PushInterval:  opts.Interval,
		InsertRequest: insertReq,
		SvcNum:        opts.ParallelNum,
		AsyncInsert:   opts.AsyncInsert,
		ServiceType:   "patterns",

		AcquireColumns: func() []service.IColPoolRes {
			return (&patternAcquirer{}).acq().toIFace()
		},
		ProcessRequest: func(v2 any, res []service.IColPoolRes) (int, []service.IColPoolRes, error) {
			patternData, ok := v2.(*model.PatternsData)
			if !ok {
				return 0, nil, fmt.Errorf("invalid request pattern ")
			}

			acquirer := (&patternAcquirer{}).fromIFace(res)
			s1 := acquirer.timestamp10m.Data.Rows()
			acquirer.timestamp10m.Data.AppendArr(patternData.MTimestamp10m)
			acquirer.fingerprint.Data.AppendArr(patternData.MFingerprint)
			acquirer.timestampS.Data.AppendArr(patternData.MTimestampS)
			acquirer.tokens.Data.AppendArr(patternData.MTokens)
			acquirer.classes.Data.AppendArr(patternData.MClasses)
			acquirer.overallCost.Data.AppendArr(patternData.MOverallCost)
			acquirer.generalizedCost.Data.AppendArr(patternData.MGeneralizedCost)
			acquirer.samplesCount.Data.AppendArr(patternData.MSamplesCount)
			acquirer.patternId.Data.AppendArr(patternData.MPatternId)
			acquirer.iterationId.Data.AppendArr(patternData.MIterationId)
			return res[0].Size() - s1, acquirer.toIFace(), nil
		},
	}
}
