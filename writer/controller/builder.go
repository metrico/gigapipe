package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	retry "github.com/avast/retry-go"
	"github.com/metrico/qryn/v5/writer/config"
	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/pattern/controller"
	"github.com/metrico/qryn/v5/writer/service"
	"github.com/metrico/qryn/v5/writer/utils"
	customErrors "github.com/metrico/qryn/v5/writer/utils/errors"
	"github.com/metrico/qryn/v5/writer/utils/helpers"
	"github.com/metrico/qryn/v5/writer/utils/logger"
	"github.com/metrico/qryn/v5/writer/utils/numbercache"
	"github.com/metrico/qryn/v5/writer/utils/promise"
	"github.com/metrico/qryn/v5/writer/utils/stat"
)

const MaxRetries = 10
const RetrySleepTimeS = 30

type MiddlewareConfig struct {
	ExtraMiddleware []BuildOption
}

// NewMiddlewareConfig generates a MiddlewareConfig from given middleware constructors.
func NewMiddlewareConfig(middlewares ...BuildOption) MiddlewareConfig {
	return MiddlewareConfig{
		ExtraMiddleware: append([]BuildOption{}, middlewares...),
	}
}

type Requester func(w http.ResponseWriter, r *http.Request) error

// Parser turns a request payload into a stream of ParserResponses. The body is
// a call-time argument because the content-type parsers are registered on the
// routes at startup, long before any request exists to read from.
type Parser func(ctx context.Context, body io.Reader, fpCache numbercache.ICache[uint64]) chan *model.ParserResponse

// BoundParser is a Parser whose input has already been supplied. IngestParsed
// takes this type rather than Parser so that it cannot be handed a parser still
// waiting for a body that IngestParsed has no way to provide.
type BoundParser func(ctx context.Context, fpCache numbercache.ICache[uint64]) chan *model.ParserResponse

// Bind supplies body to p. It is the seam between the route-registered parsers
// and the request whose payload they read.
func Bind(p Parser, body io.Reader) BoundParser {
	return func(ctx context.Context, fpCache numbercache.ICache[uint64]) chan *model.ParserResponse {
		return p(ctx, body, fpCache)
	}
}

// PreDecoded binds a parser built over an already-decoded payload, such as the
// OTLP *FromData builders. Those parsers take their input from the object
// captured at construction and never read the body, so an empty one is bound.
func PreDecoded(p Parser) BoundParser {
	return Bind(p, http.NoBody)
}

type BuildOption func(ctx *PusherCtx) *PusherCtx

type PusherCtx struct {
	PreRequest   []Requester
	PostRequest  []Requester
	Parser       map[string]Requester
	ResponseBody []byte
}

func (pusherCtx *PusherCtx) Do(w http.ResponseWriter, r *http.Request) error {
	var err error
	for _, p := range pusherCtx.PreRequest {

		err = p(w, r)
		if err != nil {
			return err
		}
	}

	err = pusherCtx.DoParse(r, w)
	if err != nil {
		return err
	}

	for _, p := range pusherCtx.PostRequest {
		err = p(w, r)
		if err != nil {
			return err
		}
	}

	return nil
}

func ErrorHandler(w http.ResponseWriter, r *http.Request, err error) {
	if e, ok := errors.AsType[*customErrors.UnMarshalError](err); ok {
		stat.AddSentMetrics("json_parse_errors", 1)
		writeErrorResponse(w, e.GetCode(), e.Error())
		return
	}
	if e, ok := errors.AsType[customErrors.IQrynError](err); ok {
		writeErrorResponse(w, e.GetCode(), e.Error())
		return
	}
	if strings.HasPrefix(err.Error(), "connection reset by peer") {
		stat.AddSentMetrics("connection_reset_by_peer", 1)
		return
	}
	logger.Error(err)
	writeErrorResponse(w, http.StatusInternalServerError, "internal server error")
}
func writeErrorResponse(w http.ResponseWriter, statusCode int, message string) {
	w.WriteHeader(statusCode)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"success": false,
		"message": message,
	})
}

func (pusherCtx *PusherCtx) DoParse(r *http.Request, w http.ResponseWriter) error {
	if len(pusherCtx.Parser) == 0 {
		return nil
	}
	contentType := r.Header.Get("Content-Type")

	var parser Requester
	for k, p := range pusherCtx.Parser {
		if strings.HasPrefix(contentType, k) {
			parser = p
			break
		}
	}
	if p, ok := pusherCtx.Parser["*"]; parser == nil && ok {
		parser = p
	}

	if parser == nil {
		return customErrors.New400Error("Content-Type not supported")
	}

	return parser(w, r.WithContext(r.Context()))
}

func Build(options ...BuildOption) func(w http.ResponseWriter, r *http.Request) {
	pusherCtx := &PusherCtx{
		Parser: map[string]Requester{},
	}
	for _, o := range options {
		pusherCtx = o(pusherCtx)
	}

	// Return a function that handles request and response and also performs error handling
	return func(w http.ResponseWriter, r *http.Request) {
		// Execute pusherCtx.Do
		err := pusherCtx.Do(w, r)
		if err != nil {
			ErrorHandler(w, r, err) // Call ErrorHandler if pusherCtx.Do returns an error
		}
	}

}

func getService(r *http.Request, name utils.ContextKey) service.IInsertServiceV2 {
	ctx := r.Context()
	svc := ctx.Value(name)
	if svc == nil {
		return nil
	}
	return svc.(service.IInsertServiceV2)
}

func doPush(req helpers.SizeGetter, insertMode int, svc service.IInsertServiceV2) *promise.Promise[uint32] {
	//	errChan := make(chan error, 1)
	p := promise.New[uint32]()
	if req == nil || svc == nil {
		return promise.Fulfilled[uint32](nil, 0)
	}
	retryAttempts := uint(config.Cloki.Setting.SYSTEM_SETTINGS.RetryAttempts)
	retryDelay := time.Duration(config.Cloki.Setting.SYSTEM_SETTINGS.RetryTimeoutS) * time.Second
	// Use the retry-go library to attempt the request up to MaxRetries times.
	go func() {
		err := retry.Do(
			func() error {
				//req.ResetResponse()
				reqPromise := svc.Request(req, insertMode)
				_, reqErr := reqPromise.Get() // Wait for the result from the svc.Request
				if reqErr != nil {
					if strings.Contains(reqErr.Error(), "dial tcp: lookup") &&
						strings.Contains(reqErr.Error(), "i/o timeout") {
						stat.AddSentMetrics("dial_tcp_lookup_timeout", 1)
					}
					logger.Error("Request error:", reqErr)
					return reqErr
				}
				return nil
			},
			retry.Attempts(retryAttempts),
			retry.Delay(retryDelay),
			retry.DelayType(retry.FixedDelay),
		)
		p.Done(0, err)
		if err != nil {
			logger.Error("Retry failed after attempts:", err)
		}
	}()
	return p
}
func getBodyStream(r *http.Request) io.Reader {
	if bodyStream, ok := r.Context().Value(utils.ContextKeyBodyStream).(io.Reader); ok {
		return bodyStream
	}
	return r.Body
}

func doLogsPattern(s *model.TimeSamplesData) {
	controller.ClusterLines(s.MMessage, s.MFingerprint, s.MTimestampNS)
}

// IngestParsed runs parser and pushes each ParserResponse to the given
// per-tenant insert services. It is the transport-agnostic core shared by the
// HTTP handlers and the gRPC receiver.
func IngestParsed(ctx context.Context, parser BoundParser, svcs InsertServices) error {
	fpNode := FPCache.DB(svcs.Node)
	var promises []*promise.Promise[uint32]
	res := parser(ctx, fpNode)
	for response := range res {
		if response.Error != nil {
			go func() {
				for range res {
				}
			}()
			return response.Error
		}
		promises = append(promises,
			doPush(response.TimeSeriesRequest, service.INSERT_MODE_SYNC, svcs.Ts),
			doPush(response.SamplesRequest, service.INSERT_MODE_SYNC, svcs.Spl),
			doPush(response.SpansAttrsRequest, service.INSERT_MODE_SYNC, svcs.SpanAttrs),
			doPush(response.SpansRequest, service.INSERT_MODE_SYNC, svcs.Spans),
			doPush(response.ProfileRequest, service.INSERT_MODE_SYNC, svcs.Profile),
		)
		if response.SamplesRequest != nil {
			doLogsPattern(response.SamplesRequest.(*model.TimeSamplesData))
		}
	}
	for _, p := range promises {
		if _, err := p.Get(); err != nil {
			return err
		}
	}
	return nil
}

func doParse(r *http.Request, parser Parser) error {
	svcs := InsertServices{
		Ts:        getService(r, utils.ContextKeyTsService),
		Spl:       getService(r, utils.ContextKeySplService),
		SpanAttrs: getService(r, utils.ContextKeySpanAttrsService),
		Spans:     getService(r, utils.ContextKeySpansService),
		Profile:   getService(r, utils.ContextKeyProfileService),
		Node:      r.Context().Value(utils.ContextKeyNode).(string),
	}
	return IngestParsed(r.Context(), Bind(parser, getBodyStream(r)), svcs)
}
