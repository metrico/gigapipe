package unmarshal

import (
	"io"

	json "github.com/json-iterator/go"
	"github.com/metrico/qryn/v4/writer/model"
)

// DecodePushRequest directly decodes json to a logproto.PushRequest
func DecodePushRequest(b io.Reader, r *model.PushRequest) error {
	return json.NewDecoder(b).Decode(r)
}

// DecodePushRequest directly decodes json to a logproto.PushRequest
func DecodePushRequestString(body []byte) (model.PushRequest, error) {

	request := model.PushRequest{}

	if err := json.Unmarshal(body, &request); err != nil {
		return request, err
	}

	/*if err := json.Unmarshal(body, r); err != nil {
		return err
	}
	*/

	return request, nil
}
