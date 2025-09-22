package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/metrico/qryn/writer/utils"
	"github.com/metrico/qryn/writer/utils/unmarshal"
)

func TargetDocV2(cfg MiddlewareConfig) func(w http.ResponseWriter, r *http.Request) {
	return Build(
		append(cfg.ExtraMiddleware,
			withTSAndSampleService,
			withParserContext(func(w http.ResponseWriter, req *http.Request, parserCtx context.Context) (context.Context, error) {
				params := getRequestParams(req)
				// Access individual parameter values
				target := params["target"]
				id := params["id"]
				firstSlash := strings.Index(target, "/")
				if firstSlash != -1 {
					target = target[:firstSlash]
				}
				_ctx := context.WithValue(parserCtx, utils.ContextKeyTarget, target)
				_ctx = context.WithValue(_ctx, utils.ContextKeyID, id)
				return _ctx, nil
			}),
			withSimpleParser("*", Parser(unmarshal.ElasticDocUnmarshalV2)),
			withOkStatusAndJSONBody(200, map[string]interface{}{
				"took":   0,
				"errors": false,
			}))...)
}

//var (
//	TargetDocV2 = Build(
//		append(WithExtraMiddlewareDefault,
//			withTSAndSampleService,
//			withParserContext(func(w http.ResponseWriter, req *http.Request, parserCtx context.Context) (context.Context, error) {
//				params := getRequestParams(req)
//				// Access individual parameter values
//				target := params["target"]
//				id := params["id"]
//				firstSlash := strings.Index(target, "/")
//				if firstSlash != -1 {
//					target = target[:firstSlash]
//				}
//				_ctx := context.WithValue(parserCtx, "target", target)
//				_ctx = context.WithValue(_ctx, "id", id)
//				return _ctx, nil
//			}),
//			withSimpleParser("*", Parser(unmarshal.ElasticDocUnmarshalV2)),
//			withOkStatusAndJSONBody(200, map[string]interface{}{
//				"took":   0,
//				"errors": false,
//			}))...)
//)

func TargetBulkV2(cfg MiddlewareConfig) func(w http.ResponseWriter, r *http.Request) {
	return Build(append(cfg.ExtraMiddleware,
		withTSAndSampleService,
		withParserContext(func(w http.ResponseWriter, req *http.Request, parserCtx context.Context) (context.Context, error) {
			params := getRequestParams(req)
			// Access individual parameter values
			target := params["target"]
			_ctx := context.WithValue(parserCtx, utils.ContextKeyTarget, target)
			return _ctx, nil
		}),
		withSimpleParser("*", Parser(unmarshal.ElasticBulkUnmarshalV2)),
		withPostRequest(func(w http.ResponseWriter, r *http.Request) error {
			w.Header().Set("x-elastic-product", "Elasticsearch")
			// Set response status code
			w.WriteHeader(http.StatusOK)
			// Prepare JSON response data
			responseData := map[string]interface{}{
				"took":   0,
				"errors": false,
			}
			// Marshal JSON response data
			responseJSON, err := json.Marshal(responseData)
			if err != nil {
				// If an error occurs during JSON marshaling, return an internal server error
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return err
			}
			// Write JSON response to the response writer
			_, err = w.Write(responseJSON)
			if err != nil {
				// If an error occurs during writing to the response writer, return an internal server error
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return err
			}
			return nil
		}))...)
}

//var TargetBulkV2 = Build(
//	append(WithExtraMiddlewareDefault,
//		withTSAndSampleService,
//		withParserContext(func(w http.ResponseWriter, req *http.Request, parserCtx context.Context) (context.Context, error) {
//			params := getRequestParams(req)
//			// Access individual parameter values
//			target := params["target"]
//			_ctx := context.WithValue(parserCtx, "target", target)
//			return _ctx, nil
//		}),
//		withSimpleParser("*", Parser(unmarshal.ElasticBulkUnmarshalV2)),
//		withPostRequest(func(r *http.Request, w http.ResponseWriter) error {
//			w.Header().Set("x-elastic-product", "Elasticsearch")
//			// Set response status code
//			w.WriteHeader(http.StatusOK)
//			// Prepare JSON response data
//			responseData := map[string]interface{}{
//				"took":   0,
//				"errors": false,
//			}
//			// Marshal JSON response data
//			responseJSON, err := json.Marshal(responseData)
//			if err != nil {
//				// If an error occurs during JSON marshaling, return an internal server error
//				http.Error(w, err.Error(), http.StatusInternalServerError)
//				return err
//			}
//			// Write JSON response to the response writer
//			_, err = w.Write(responseJSON)
//			if err != nil {
//				// If an error occurs during writing to the response writer, return an internal server error
//				http.Error(w, err.Error(), http.StatusInternalServerError)
//				return err
//			}
//			return nil
//		}))...)

func getRequestParams(r *http.Request) map[string]string {
	params := make(map[string]string)
	ctx := r.Context()
	if ctxParams, ok := ctx.Value(utils.ContextKeyParams).(map[string]string); ok {
		for key, value := range ctxParams {
			params[key] = value
		}
	}
	return params
}
