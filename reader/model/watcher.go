package model

type QueryRangeOutput struct {
	Str string
	Err error
}

type IWatcher interface {
	Close()
	GetRes() chan QueryRangeOutput
	Done() <-chan struct{}
}

// IndexStatsResult holds the response for GET /loki/api/v1/index/stats.
type IndexStatsResult struct {
	Streams int64 `json:"streams"`
	Chunks  int64 `json:"chunks"`
	Bytes   int64 `json:"bytes"`
	Entries int64 `json:"entries"`
}
