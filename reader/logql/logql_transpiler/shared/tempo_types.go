package shared

import "github.com/metrico/qryn/v4/reader/model"

type GenericTraceRequestProcessor[T any] interface {
	Process(*PlannerContext) (chan []T, error)
}

type TraceRequestProcessor interface {
	Process(*PlannerContext) (chan []model.TraceInfo, error)
}
