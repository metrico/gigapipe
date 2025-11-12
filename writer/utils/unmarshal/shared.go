package unmarshal

import (
	"time"

	"github.com/metrico/qryn/v4/writer/model"
	"github.com/metrico/qryn/v4/writer/utils/metadata"
)

type timeSeriesAndSamples struct {
	ts        *model.TimeSeriesData
	spl       *model.TimeSamplesData
	metadata  *model.MetadataData
	size      int
	c         chan *model.ParserResponse
	meta      string
}

func (t *timeSeriesAndSamples) reset() {
	t.size = 0
	t.ts = &model.TimeSeriesData{
		MDate:        make([]time.Time, 0, 100),
		MLabels:      make([]string, 0, 100),
		MFingerprint: make([]uint64, 0, 100),
		MType:        make([]uint8, 0, 100),
		MMeta:        t.meta,
	}
	t.spl = &model.TimeSamplesData{
		MTimestampNS: make([]int64, 0, 1000),
		MFingerprint: make([]uint64, 0, 1000),
		MMessage:     make([]string, 0, 1000),
		MValue:       make([]float64, 0, 1000),
	}
	t.metadata = &model.MetadataData{
		MetricNames:  make([]string, 0, 10),
		MetadataJSON: make([]string, 0, 10),
	}
}

func (t *timeSeriesAndSamples) flush() {
	// Prepare metadata request if we have any metadata
	var metadataReq *model.MetadataData
	if len(t.metadata.MetricNames) > 0 {
		metadataReq = t.metadata
		// Reset for next batch
		t.metadata = &model.MetadataData{
			MetricNames:  make([]string, 0, 10),
			MetadataJSON: make([]string, 0, 10),
		}
	}

	t.c <- &model.ParserResponse{
		TimeSeriesRequest: t.ts,
		SamplesRequest:    t.spl,
		MetadataRequest:   metadataReq,
	}
}

func newTimeSeriesAndSamples(c chan *model.ParserResponse,
	meta string) *timeSeriesAndSamples {
	res := &timeSeriesAndSamples{
		c:    c,
		meta: meta,
	}
	res.reset()
	return res
}

func fastFillArray[T any](len int, val T) []T {
	res := make([]T, len)
	res[0] = val
	_len := 1
	for _len < len {
		copy(res[_len:], res[:_len])
		_len <<= 1
	}
	return res
}
