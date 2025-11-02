package model

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/metrico/qryn/v4/logger"
)

type PushRequest struct {
	Streams []Stream `json:"streams"`
}
type Stream struct {
	Labels  string            `json:"labels"`
	Stream  map[string]string `json:"stream"`
	Entries []Entry           `json:"entries"`
	Values  [][]string        `json:"values"`
}

// LokiTime is our magic type
type LokiTime struct {
	int64
}

// Entry is a log entry with a timestamp.
type Entry struct {
	Timestamp LokiTime `json:"ts"`
	Line      string   `json:"line"`
}

type LabelRules struct {
	Label, Cond, Value string
}

func FromNano(nanos int64) LokiTime {
	return LokiTime{nanos}
}

func (l *LokiTime) GetNanos() int64 {
	return l.int64
}

// UnmarshalJSON is the method that satisfies the Unmarshaller interface
func (u *LokiTime) UnmarshalJSON(b []byte) error {
	// 2021-12-26T16:00:06.944Z
	var err error
	if b != nil {
		var timestamp int64
		val, _ := strconv.Unquote(string(b))
		if strings.ContainsAny(val, ":-TZ") {
			t, e := time.Parse(time.RFC3339, val)
			if e != nil {
				logger.Debug("ERROR unmarshaling this string: ", e.Error())
				return err
			}
			timestamp = (t.UTC().UnixNano())
		} else {
			timestamp, err = strconv.ParseInt(val, 10, 64)
			if err != nil {
				logger.Debug("ERROR unmarshaling this NS: ", val, err)
				return err
			}
		}
		u.int64 = timestamp
		return nil
	} else {
		err = fmt.Errorf("bad byte array for Unmarshaling")
		logger.Debug("bad data: ", err)
		return err
	}
}
