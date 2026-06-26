package controller

import (
	"bytes"
	"context"

	"github.com/metrico/qryn/v4/writer/service"
	"github.com/metrico/qryn/v4/writer/utils/promise"
	"github.com/metrico/qryn/v4/writer/utils/proto/prompb"
	"github.com/metrico/qryn/v4/writer/utils/unmarshal"
	"google.golang.org/protobuf/proto"
)

// PushPromWriteRequest ingests a Prometheus remote-write request in-process,
// reusing the metrics parser (which fingerprints labels) and pushing the built
// time_series/samples models to the static insert registry. It is the
// in-process write-back path for recording rules: no HTTP, snappy, or auth.
//
// The writer module must be initialized first, so Registry and FPCache are set.
func PushPromWriteRequest(ctx context.Context, wr *prompb.WriteRequest) error {
	if wr == nil || len(wr.GetTimeseries()) == 0 {
		return nil
	}

	data, err := proto.Marshal(wr)
	if err != nil {
		return err
	}

	tsSvc, err := Registry.GetTimeSeriesService("")
	if err != nil {
		return err
	}
	splSvc, err := Registry.GetSamplesService("")
	if err != nil {
		return err
	}
	node := tsSvc.GetNodeName()

	res := unmarshal.UnmarshallMetricsWriteProtoV2(ctx, bytes.NewReader(data), FPCache.DB(node))

	var promises []*promise.Promise[uint32]
	for response := range res {
		if response.Error != nil {
			// Drain remaining responses so the parser goroutine can exit.
			go func() {
				for range res {
				}
			}()
			return response.Error
		}
		promises = append(promises,
			doPush(response.TimeSeriesRequest, service.INSERT_MODE_SYNC, tsSvc),
			doPush(response.SamplesRequest, service.INSERT_MODE_SYNC, splSvc),
		)
	}
	for _, p := range promises {
		if _, err := p.Get(); err != nil {
			return err
		}
	}
	return nil
}
