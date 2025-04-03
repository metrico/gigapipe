package controllerv1

import (
	"context"
	"errors"
	"github.com/metrico/qryn/writer/utils/unmarshal"
	"net/http"
)

func PushProfileV2(cfg MiddlewareConfig) func(w http.ResponseWriter, r *http.Request) {
	return Build(
		append(cfg.ExtraMiddleware,
			withTSAndSampleService,
			withParserContext(func(w http.ResponseWriter, req *http.Request, parserCtx context.Context) (context.Context, error) {
				fromValue := req.URL.Query().Get("from")

				if fromValue == "" {
					return nil, errors.New("please provide from value")
				}

				nameValue := req.URL.Query().Get("name")

				if nameValue == "" {
					return nil, errors.New("please provide name value")
				}
				untilValue := req.URL.Query().Get("until")

				if untilValue == "" {
					return nil, errors.New("please provide until value")
				}

				_ctx := context.WithValue(parserCtx, "from", fromValue)
				_ctx = context.WithValue(_ctx, "name", nameValue)
				_ctx = context.WithValue(_ctx, "until", untilValue)
				return _ctx, nil
			}),
			withSimpleParser("*", Parser(unmarshal.UnmarshalProfileProtoV2)),
			withOkStatusAndBody(200, []byte("{}")))...)
}
