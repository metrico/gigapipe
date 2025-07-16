package controller

import (
	"context"
	"net/http"

	"github.com/metrico/qryn/writer/utils/unmarshal"
)

func PushDatadogV2(cfg MiddlewareConfig) func(w http.ResponseWriter, r *http.Request) {
	return Build(
		append(cfg.ExtraMiddleware,
			withTSAndSampleService,
			withParserContext(func(w http.ResponseWriter, req *http.Request, parserCtx context.Context) (context.Context, error) {

				ddsource := req.URL.Query().Get("ddsource")
				if ddsource == "" {
					ddsource = "unknown"
				}
				return context.WithValue(parserCtx, ContextKeyDDSource, ddsource), nil
			}),
			withSimpleParser("application/json", Parser(unmarshal.UnmarshallDatadogV2JSONV2)),
			withOkStatusAndBody(202, []byte("{}")))...)
}

//var PushDatadogV2 = Build(
//	append(WithExtraMiddlewareDefault,
//		withTSAndSampleService,
//		withSimpleParser("application/json", Parser(unmarshal.UnmarshallDatadogV2JSONV2)),
//		withOkStatusAndBody(202, []byte("ok")),
//	)...,
//)

func PushCfDatadogV2(cfg MiddlewareConfig) func(w http.ResponseWriter, r *http.Request) {
	return Build(
		append(cfg.ExtraMiddleware,
			withTSAndSampleService,
			withParserContext(func(w http.ResponseWriter, req *http.Request, parserCtx context.Context) (context.Context, error) {
				ddsource := req.URL.Query().Get("ddsource")
				if ddsource == "" {
					ddsource = "unknown"
				}
				return context.WithValue(parserCtx, ContextKeyDDSource, ddsource), nil
			}),
			withSimpleParser("*", Parser(unmarshal.UnmarshallDatadogCFJSONV2)),
			withOkStatusAndBody(202, []byte("{}")))...)
}

//var PushCfDatadogV2 = Build(
//	append(WithExtraMiddlewareDefault,
//		withTSAndSampleService,
//		withParserContext(func(w http.ResponseWriter, req *http.Request, parserCtx context.Context) (context.Context, error) {
//
//			ddsource := req.URL.Query().Get("ddsource")
//			if ddsource == "" {
//				ddsource = "unknown"
//			}
//			return context.WithValue(parserCtx, "ddsource", ddsource), nil
//		}),
//		withSimpleParser("*", Parser(unmarshal.UnmarshallDatadogCFJSONV2)),
//		withOkStatusAndBody(200, []byte("{}")))...)

func PushDatadogMetricsV2(cfg MiddlewareConfig) func(w http.ResponseWriter, r *http.Request) {

	return Build(
		append(cfg.ExtraMiddleware,
			withTSAndSampleService,
			withSimpleParser("application/json", Parser(unmarshal.UnmarshallDatadogMetricsV2JSONV2)),
			withOkStatusAndBody(202, []byte("{}")))...)

}

//
//var PushDatadogMetricsV2 = Build(
//	append(WithExtraMiddlewareDefault,
//		withTSAndSampleService,
//		withSimpleParser("application/json", Parser(unmarshal.UnmarshallDatadogMetricsV2JSONV2)),
//		withOkStatusAndBody(202, []byte("{}")))...)
