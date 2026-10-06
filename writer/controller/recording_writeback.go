package controller

import (
	"context"

	"github.com/metrico/qryn/v5/writer/utils/unmarshal"
)

// PushMetricSeries writes series through the metric entry point into the
// metric insert services and returns once the inserts complete. The writer
// module must be initialized first, so Registry and the caches are set.
func PushMetricSeries(ctx context.Context, series []unmarshal.MetricSeries) error {
	if len(series) == 0 {
		return nil
	}
	svcs, err := ResolveMetricServices("")
	if err != nil {
		return err
	}
	return IngestParsed(ctx, PreDecoded(Parser(unmarshal.MetricSeriesParser(series))), svcs)
}
