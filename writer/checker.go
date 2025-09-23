package writer

import (
	"fmt"
	"strconv"
)

type SetupState struct {
	Version         string
	Type            string
	Shards          int
	SamplesChannels int
	TSChannels      int
	Preforking      bool
	Forks           int
}

func (s SetupState) ToLogLines() []string {
	shards := strconv.FormatInt(int64(s.Shards), 10)
	if s.Shards == 0 {
		shards = "can't retrieve"
	}
	return []string{
		"QRYN-WRITER SETTINGS:",
		"qryn-writer version: " + s.Version,
		"clickhouse setup type: " + s.Type,
		"shards: " + shards,
		"samples channels: " + strconv.FormatInt(int64(s.SamplesChannels), 10),
		"time-series channels: " + strconv.FormatInt(int64(s.TSChannels), 10),
		fmt.Sprintf("preforking: %v", s.Preforking),
		"forks: " + strconv.FormatInt(int64(s.Forks), 10),
	}
}
