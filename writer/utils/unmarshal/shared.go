package unmarshal

import (
	"time"

	"github.com/metrico/qryn/v5/writer/model"
)

type timeSeriesAndSamples struct {
	ts   *model.TimeSeriesData
	spl  *model.TimeSamplesData
	size int
	c    chan *model.ParserResponse
	meta string
}

func (t *timeSeriesAndSamples) reset() {
	t.size = 0
	t.ts = &model.TimeSeriesData{
		MDate:        make([]time.Time, 0, 100),
		MLabels:      make([]string, 0, 100),
		MFingerprint: make([]uint64, 0, 100),
		MType:        make([]uint8, 0, 100),
		MMeta:        t.meta,
		MMetadata:    make([]string, 0, 100),
	}
	t.spl = &model.TimeSamplesData{
		MTimestampNS: make([]int64, 0, 1000),
		MFingerprint: make([]uint64, 0, 1000),
		MMessage:     make([]string, 0, 1000),
		MValue:       make([]float64, 0, 1000),
	}
}

func (t *timeSeriesAndSamples) flush() {
	t.c <- &model.ParserResponse{
		TimeSeriesRequest: t.ts,
		SamplesRequest:    t.spl,
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
