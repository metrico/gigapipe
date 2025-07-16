package model

type HttpResponse struct {
	Id          string
	Respone     []byte
	Err         error
	InstanceTag string
	EndpointTag string
	TimeStamp   int64
}
