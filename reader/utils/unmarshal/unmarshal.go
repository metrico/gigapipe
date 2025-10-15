package unmarshal

import (
	"io"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/metrico/qryn/v4/reader/model"
)

var jsonApi = jsoniter.ConfigCompatibleWithStandardLibrary

// DecodePushRequest directly decodes json to a logproto.PushRequest
func DecodePushRequest(b io.Reader) (model.PushRequest, error) {

	request := model.PushRequest{}

	if err := jsoniter.NewDecoder(b).Decode(&request); err != nil {
		return request, err
	}

	return request, nil
}

// DecodePushRequest directly decodes json to a logproto.PushRequest
func DecodePushRequestString(body []byte) (model.PushRequest, error) {

	request := model.PushRequest{}

	if err := jsonApi.Unmarshal(body, &request); err != nil {
		return request, err
	}

	return request, nil
}

// DecodePushRequest directly decodes json to a logproto.PushRequest
func MarshalLabelsPushRequestString(labels []model.Label) ([]byte, error) {

	strArr := []string{}

	for _, s := range labels {
		strArr = append(strArr, s.Key+"=\""+s.Value+"\"")
	}

	return []byte(strings.Join(strArr, ",")), nil
}

// DecodePushRequest directly decodes json to a logproto.PushRequest
func MarshalArrayLabelsPushRequestString(labels []string) ([]byte, error) {

	data, err := jsonApi.Marshal(labels)
	if err != nil {
		return nil, err
	}
	return data, err
}

/*
// NewPushRequest constructs a logproto.PushRequest from a PushRequest
func NewPushRequest(r loghttp.PushRequest) logproto.PushRequest {
	ret := logproto.PushRequest{
		Streams: make([]logproto.Stream, len(r.Streams)),
	}

	for i, s := range r.Streams {
		ret.Streams[i] = NewStream(s)
	}

	return ret
}

// NewPushRequest constructs a logproto.PushRequest from a PushRequest
func NewPushRequestLog(r model.PushRequest) logproto.PushRequest {
	ret := logproto.PushRequest{
		Streams: make([]logproto.Stream, len(r.Streams)),
	}
	for i, s := range r.Streams {
		ret.Streams[i] = NewStreamLog(&s)
	}

	return ret
}

// NewStream constructs a logproto.Stream from a Stream
func NewStream(s *loghttp.Stream) logproto.Stream {
	return logproto.Stream{
		Entries: *(*[]logproto.Entry)(unsafe.Pointer(&s.Entries)),
		Labels:  s.Labels.String(),
	}
}

// NewStream constructs a logproto.Stream from a Stream
func NewStreamLog(s *model.Stream) logproto.Stream {
	return logproto.Stream{
		Entries: *(*[]logproto.Entry)(unsafe.Pointer(&s.Entries)),
		Labels:  s.Labels,
	}
}

// WebsocketReader knows how to read message to a websocket connection.
type WebsocketReader interface {
	ReadMessage() (int, []byte, error)
}

// ReadTailResponseJSON unmarshals the loghttp.TailResponse from a websocket reader.
func ReadTailResponseJSON(r *loghttp.TailResponse, reader WebsocketReader) error {
	_, data, err := reader.ReadMessage()
	if err != nil {
		return err
	}
	return jsoniter.Unmarshal(data, r)
}
*/
