package controller

import (
	"bytes"
	"context"
	"io"
	"net/http"

	"github.com/metrico/qryn/writer/utils"
	"github.com/metrico/qryn/writer/utils/unmarshal"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

type TempoController struct {
}

func PushV2(cfg MiddlewareConfig) func(w http.ResponseWriter, r *http.Request) {
	return Build(
		append(cfg.ExtraMiddleware,
			withTracesService,
			withSimpleParser("ndjson", Parser(unmarshal.UnmarshalZipkinNDJSONV2)),
			withSimpleParser("*", Parser(unmarshal.UnmarshalZipkinJSONV2)),
			withOkStatusAndBody(202, nil))...)
}

var ClickhousePushV2 = PushV2

//var PushV2 = Build(
//	append(WithExtraMiddlewareTempo,
//		withTracesService,
//		withSimpleParser("ndjson", Parser(unmarshal.UnmarshalZipkinNDJSONV2)),
//		withSimpleParser("*", Parser(unmarshal.UnmarshalZipkinJSONV2)),
//		withOkStatusAndBody(202, nil))...)

//var ClickhousePushV2 = PushV2

func OTLPPushV2(cfg MiddlewareConfig) func(w http.ResponseWriter, r *http.Request) {
	return Build(
		append(cfg.ExtraMiddleware,
			withTracesService,
			WithPreRequest(func(w http.ResponseWriter, r *http.Request) error {
				// Read the request body
				body, err := io.ReadAll(r.Body)
				if err != nil {
					return err
				}
				defer r.Body.Close()

				// Create a new request context with the modified body
				ctx := context.WithValue(r.Context(), utils.ContextKeyBodyStream, bytes.NewReader(body))
				*r = *r.WithContext(ctx)
				return nil

			}),
			withSimpleParser("*", Parser(unmarshal.UnmarshalOTLPV2)),
			withOkStatusAndBody(200, func() []byte {
				res, _ := ptraceotlp.NewExportResponse().MarshalProto()
				return res
			}()),
		)...)

}

//var OTLPPushV2 = Build(
//	append(WithExtraMiddlewareTempo,
//		withTracesService,
//		withPreRequest(func(r *http.Request, w http.ResponseWriter) error {
//			// Read the request body
//			body, err := io.ReadAll(r.Body)
//			if err != nil {
//				return err
//			}
//			defer r.Body.Close()
//
//			// Create a new request context with the modified body
//			ctx := context.WithValue(r.Context(), "bodyStream", bytes.NewReader(body))
//			*r = *r.WithContext(ctx)
//			return nil
//
//		}),
//		withSimpleParser("*", Parser(unmarshal.UnmarshalOTLPV2)),
//		withOkStatusAndBody(200, func() []byte {
//			res, _ := ptraceotlp.NewResponse().MarshalProto()
//			return res
//		}()),
//	)...)
