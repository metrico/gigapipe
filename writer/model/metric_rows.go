package model

// MetricSamplesData is a batch of metric_samples_in rows.
type MetricSamplesData struct {
	MFingerprint     []uint64
	MTimestampMs     []int64
	MValue           []float64
	MPrevTimestampMs []int64
	MPrevValue       []float64
	MAggregate       []uint8
	Size             int
}

func (d *MetricSamplesData) GetSize() int64 { return int64(d.Size) }

// MetricSeriesData is a batch of metric_series rows. MLabels holds the full
// label set, __name__ included.
type MetricSeriesData struct {
	MName        []string
	MFingerprint []uint64
	MLabels      []map[string]string
	MFirstSeenMs []int64
	MLastSeenMs  []int64
	Size         int
}

func (d *MetricSeriesData) GetSize() int64 { return int64(d.Size) }

// MetricMetadataData is a batch of metric_metadata rows.
type MetricMetadataData struct {
	MName        []string
	MType        []string
	MHelp        []string
	MUnit        []string
	MUpdatedAtMs []int64
	Size         int
}

func (d *MetricMetadataData) GetSize() int64 { return int64(d.Size) }

// MetricExemplarsData is a batch of metric_exemplars rows. MLabels is the
// exemplar's label set encoded as time_series.labels is.
type MetricExemplarsData struct {
	MFingerprint []uint64
	MTimestampMs []int64
	MValue       []float64
	MTraceID     []string
	MLabels      []string
	Size         int
}

func (d *MetricExemplarsData) GetSize() int64 { return int64(d.Size) }
