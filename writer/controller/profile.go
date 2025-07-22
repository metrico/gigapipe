package controller

import (
	"context"
	"errors"
	"net/http"

	"github.com/metrico/qryn/writer/utils"
	"github.com/metrico/qryn/writer/utils/unmarshal"
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

				_ctx := context.WithValue(parserCtx, utils.ContextKeyFrom, fromValue)
				_ctx = context.WithValue(_ctx, utils.ContextKeyName, nameValue)
				_ctx = context.WithValue(_ctx, utils.ContextKeyUntil, untilValue)
				return _ctx, nil
			}),
			// Register parser for multipart/form-data content type
			withSimpleParser("multipart/form-data", Parser(unmarshal.UnmarshalProfileProtoV2)),
			// Register parser for binary/octet-stream content type
			withSimpleParser("binary/octet-stream", Parser(unmarshal.UnmarshalBinaryStreamProfileProtoV2)),
			//withSimpleParser("*", Parser(unmarshal.UnmarshalProfileProtoV2)),
			withOkStatusAndBody(200, []byte("{}")))...)
}
