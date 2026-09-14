package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/metrico/qryn/v5/writer/utils"
	"github.com/metrico/qryn/v5/writer/utils/unmarshal"
)

func TargetDocV2(cfg MiddlewareConfig) func(w http.ResponseWriter, r *http.Request) {
	return Build(
		append(cfg.ExtraMiddleware,
			withTSAndSampleService,
			withParserContext(func(w http.ResponseWriter, req *http.Request, parserCtx context.Context) (context.Context, error) {
				vars := mux.Vars(req)
				target := vars["target"]
				firstSlash := strings.Index(target, "/")
				if firstSlash != -1 {
					target = target[:firstSlash]
				}
				_ctx := context.WithValue(parserCtx, utils.ContextKeyTarget, target)
				// An absent {id} must not reach the parser: an empty value there
				// is still a value, and the document gets an _id="" label.
				if id := vars["id"]; id != "" {
					_ctx = context.WithValue(_ctx, utils.ContextKeyID, id)
				}
				return _ctx, nil
			}),
			withSimpleParser("*", Parser(unmarshal.ElasticDocUnmarshalV2)),
			withOkStatusAndJSONBody(200, map[string]any{
				"took":   0,
				"errors": false,
			}))...)
}

func TargetBulkV2(cfg MiddlewareConfig) func(w http.ResponseWriter, r *http.Request) {
	return Build(append(cfg.ExtraMiddleware,
		withTSAndSampleService,
		withParserContext(func(w http.ResponseWriter, req *http.Request, parserCtx context.Context) (context.Context, error) {
			_ctx := context.WithValue(parserCtx, utils.ContextKeyTarget, mux.Vars(req)["target"])
			return _ctx, nil
		}),
		withSimpleParser("*", Parser(unmarshal.ElasticBulkUnmarshalV2)),
		withPostRequest(func(w http.ResponseWriter, r *http.Request) error {
			w.Header().Set("x-elastic-product", "Elasticsearch")
			// Set response status code
			w.WriteHeader(http.StatusOK)
			// Prepare JSON response data
			responseData := map[string]any{
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
