package controller

import (
	"bytes"
	"context"

	"github.com/metrico/qryn/v5/writer/utils/proto/prompb"
	"github.com/metrico/qryn/v5/writer/utils/unmarshal"
	"google.golang.org/protobuf/proto"
)

// PushPromWriteRequest ingests a Prometheus remote-write request in-process,
// reusing the metrics parser (which fingerprints labels) and routing it
// through IngestParsed like every other transport. It is the in-process
// write-back path for recording rules: no HTTP, snappy, or auth.
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

	svcs, err := ResolveMetricServices("")
	if err != nil {
		return err
	}
	return IngestParsed(ctx, Bind(Parser(unmarshal.UnmarshallMetricsWriteProtoV2), bytes.NewReader(data)), svcs)
}
