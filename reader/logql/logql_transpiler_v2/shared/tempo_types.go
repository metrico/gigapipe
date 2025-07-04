package shared

import "github.com/metrico/qryn/reader/model"

type GenericTraceRequestProcessor[T any] interface {
	Process(*PlannerContext) (chan []T, error)
}

type TraceRequestProcessor interface {
	Process(*PlannerContext) (model.TraceQLResponse, error)
}
