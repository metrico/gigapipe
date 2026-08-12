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
	oid  string
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
		MOid:         t.oid,
	}
	t.spl = &model.TimeSamplesData{
		MTimestampNS: make([]int64, 0, 1000),
		MFingerprint: make([]uint64, 0, 1000),
		MMessage:     make([]string, 0, 1000),
		MValue:       make([]float64, 0, 1000),
		MOid:         t.oid,
	}
}

func (t *timeSeriesAndSamples) flush() {
	t.c <- &model.ParserResponse{
		TimeSeriesRequest: t.ts,
		SamplesRequest:    t.spl,
	}
}

func newTimeSeriesAndSamples(c chan *model.ParserResponse,
	meta string, oid string) *timeSeriesAndSamples {
	res := &timeSeriesAndSamples{
		c:    c,
		meta: meta,
		oid:  oid,
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
