package util

import (
	"math"
	"time"
	"unsafe"

	"github.com/prometheus/common/model"
)

// ModelLabelSetToMap convert a model.LabelSet to a map[string]string
func ModelLabelSetToMap(m model.LabelSet) map[string]string {
	if len(m) == 0 {
		return map[string]string{}
	}
	return *(*map[string]string)(unsafe.Pointer(&m))
}

// MapToModelLabelSet converts a map into a model.LabelSet
func MapToModelLabelSet(m map[string]string) model.LabelSet {
	if len(m) == 0 {
		return model.LabelSet{}
	}
	return *(*map[model.LabelName]model.LabelValue)(unsafe.Pointer(&m))
}

// RoundToMilliseconds returns milliseconds precision time from nanoseconds.
// from will be rounded down to the nearest milliseconds while through is rounded up.
func RoundToMilliseconds(from, through time.Time) (model.Time, model.Time) {
	return model.Time(int64(math.Floor(float64(from.UnixNano()) / float64(time.Millisecond)))),
		model.Time(int64(math.Ceil(float64(through.UnixNano()) / float64(time.Millisecond))))
}
