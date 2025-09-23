package controller

import (
	"context"
	"fmt"
	"net/http"
	"time"

	helputils "github.com/metrico/qryn/writer/utils"
	"github.com/metrico/qryn/writer/utils/errors"
	"github.com/metrico/qryn/writer/utils/unmarshal"
)

// swagger:route GET /push Data PushData
//
// # Returns data from server in array
//
// ---
//
//	    Consumes:
//	    - application/json
//
//		   Produces:
//		   - application/json
//
//		   Security:
//		   - JWT
//	    - ApiKeyAuth
//
// SecurityDefinitions:
// JWT:
//
//	type: apiKey
//	name: Authorization
//	in: header
//
// ApiKeyAuth:
//
//	type: apiKey
//	in: header
//	name: Auth-Token
//
// /
//
//	Responses:
//	  201: body:TableUserList
//	  400: body:FailureResponse

func PushStreamV2(cfg MiddlewareConfig) func(w http.ResponseWriter, r *http.Request) {
	return Build(
		append(cfg.ExtraMiddleware,
			withTSAndSampleService,
			withSimpleParser("*", Parser(unmarshal.DecodePushRequestStringV2)),
			withComplexParser("application/x-protobuf",
				Parser(unmarshal.UnmarshalProtoV2),
				withUnsnappyRequest),
			withOkStatusAndBody(204, nil),
		)...,
	)
}

//var PushStreamV2 = Build(
//	append(WithExtraMiddlewareDefault,
//		withTSAndSampleService,
//		withSimpleParser("*", Parser(unmarshal.DecodePushRequestStringV2)),
//		withComplexParser("application/x-protobuf",
//			Parser(unmarshal.UnmarshalProtoV2),
//			withUnsnappyRequest),
//		withOkStatusAndBody(204, nil),
//	)...)

func PushInfluxV2(cfg MiddlewareConfig) func(w http.ResponseWriter, r *http.Request) {
	return Build(
		append(cfg.ExtraMiddleware,
			withTSAndSampleService,
			withParserContext(func(w http.ResponseWriter, req *http.Request, parserCtx context.Context) (context.Context, error) {
				strPrecision := req.URL.Query().Get("precision")
				if strPrecision == "" {
					strPrecision = "ns"
				}
				var precision time.Duration
				switch strPrecision {
				case "ns":
					precision = time.Nanosecond
				case "us":
					precision = time.Microsecond
				case "ms":
					precision = time.Millisecond
				case "s":
					precision = time.Second
				default:
					return nil, errors.New400Error(fmt.Sprintf("Invalid precision %s", strPrecision))
				}
				ctx := req.Context()
				ctx = context.WithValue(ctx, helputils.ContextKeyPrecision, precision)
				return ctx, nil
			}),
			withSimpleParser("*", Parser(unmarshal.UnmarshalInfluxDBLogsV2)),
			withPostRequest(func(w http.ResponseWriter, r *http.Request) error {
				w.WriteHeader(http.StatusNoContent)
				// Write "Ok" as the response body
				_, _ = w.Write([]byte("Ok"))

				return nil
			}))...)
}

//var PushInfluxV2 = Build(
//	append(WithExtraMiddlewareDefault,
//		withTSAndSampleService,
//		withParserContext(func(w http.ResponseWriter, req *http.Request, parserCtx context.Context) (context.Context, error) {
//			strPrecision := req.URL.Query().Get("precision")
//			if strPrecision == "" {
//				strPrecision = "ns"
//			}
//			var precision time.Duration
//			switch strPrecision {
//			case "ns":
//				precision = time.Nanosecond
//			case "us":
//				precision = time.Microsecond
//			case "ms":
//				precision = time.Millisecond
//			case "s":
//				precision = time.Second
//			default:
//				return nil, errors.New400Error(fmt.Sprintf("Invalid precision %s", strPrecision))
//			}
//			ctx := req.Context()
//			ctx = context.WithValue(ctx, "precision", precision)
//			return ctx, nil
//		}),
//		withSimpleParser("*", Parser(unmarshal.UnmarshalInfluxDBLogsV2)),
//		withPostRequest(func(r *http.Request, w http.ResponseWriter) error {
//			w.WriteHeader(http.StatusNoContent)
//			// Write "Ok" as the response body
//			_, _ = w.Write([]byte("Ok"))
//
//			return nil
//		}))...)

func OTLPLogsV2(cfg MiddlewareConfig) func(w http.ResponseWriter, r *http.Request) {
	return Build(
		append(cfg.ExtraMiddleware,
			withTSAndSampleService,
			withSimpleParser("*", Parser(unmarshal.UnmarshalOTLPLogsV2)),
			withPostRequest(func(w http.ResponseWriter, r *http.Request) error {
				w.WriteHeader(http.StatusNoContent)
				// Write "Ok" as the response body
				_, _ = w.Write([]byte("Ok"))
				return nil
			}))...)
}

//var OTLPLogsV2 = Build(
//	append(WithExtraMiddlewareDefault,
//		withTSAndSampleService,
//		withSimpleParser("*", Parser(unmarshal.UnmarshalOTLPLogsV2)),
//		withPostRequest(func(r *http.Request, w http.ResponseWriter) error {
//			w.WriteHeader(http.StatusNoContent)
//			// Write "Ok" as the response body
//			_, _ = w.Write([]byte("Ok"))
//			return nil
//		}))...)
