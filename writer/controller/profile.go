package controller

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/metrico/qryn/v4/writer/utils"
	customErrors "github.com/metrico/qryn/v4/writer/utils/errors"
	"github.com/metrico/qryn/v4/writer/utils/unmarshal"
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

func OTLPProfilesV2(cfg MiddlewareConfig) func(w http.ResponseWriter, r *http.Request) {
	return Build(
		append(cfg.ExtraMiddleware,
			withTSAndSampleService,
			// Reject JSON payloads with 415; only protobuf is supported.
			withParserContext(func(w http.ResponseWriter, req *http.Request, parserCtx context.Context) (context.Context, error) {
				if strings.Contains(req.Header.Get("Content-Type"), "application/json") {
					return nil, &customErrors.QrynError{
						Code:    http.StatusUnsupportedMediaType,
						Message: "OTLP profiles: JSON not supported, use application/x-protobuf",
					}
				}
				return parserCtx, nil
			}),
			withSimpleParser("application/x-protobuf", Parser(unmarshal.UnmarshalOTLPProfilesProtoV2)),
			withSimpleParser("*", Parser(unmarshal.UnmarshalOTLPProfilesProtoV2)),
			withOkStatusAndBody(200, []byte("{}")))...)
}
