package controllerv1

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"github.com/golang/snappy"
	"github.com/metrico/qryn/writer/ch_wrapper"
	custom_errors "github.com/metrico/qryn/writer/utils/errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var DbClient ch_wrapper.IChClient

type cacheItem struct {
	value          uint64    // Assuming it stores an int64 value for logs data
	timestamp      time.Time // Timestamp when the item was cached
	organizationID string
}

var cacheData sync.Map

type ResponseWriter struct {
	header http.Header
	status int
	body   []byte
}

func WithPreRequest(preRequest Requester) BuildOption {
	return func(ctx *PusherCtx) *PusherCtx {
		ctx.PreRequest = append(ctx.PreRequest, preRequest)
		return ctx
	}
}

func withPostRequest(postRequest Requester) BuildOption {
	return func(ctx *PusherCtx) *PusherCtx {
		ctx.PostRequest = append(ctx.PostRequest, postRequest)
		return ctx
	}
}

func withSimpleParser(contentType string, parser Parser) BuildOption {
	return func(ctx *PusherCtx) *PusherCtx {
		ctx.Parser[contentType] = func(w http.ResponseWriter, r *http.Request) error {
			// Assuming doParse function signature is compatible with Parser
			return doParse(r, parser)

		}
		return ctx
	}
}

func withComplexParser(contentType string, parser Parser, options ...BuildOption) BuildOption {
	pusherCtx := &PusherCtx{
		Parser: make(map[string]Requester),
	}

	// Apply options to pusherCtx
	for _, o := range options {
		pusherCtx = o(pusherCtx)
	}

	// Define parser for contentType
	pusherCtx.Parser["*"] = func(w http.ResponseWriter, r *http.Request) error {
		return doParse(r, parser)
	}

	// Return BuildOption function
	return func(ctx *PusherCtx) *PusherCtx {
		// Set the parser for contentType in ctx
		ctx.Parser[contentType] = pusherCtx.Do
		return ctx
	}
}

func withOkStatusAndBody(status int, body []byte) BuildOption {
	return func(ctx *PusherCtx) *PusherCtx {
		ctx.PostRequest = append(ctx.PostRequest, func(w http.ResponseWriter, r *http.Request) error {
			w.WriteHeader(status)
			w.Write(body)
			return nil
		})
		return ctx
	}
}

func withOkStatusAndJSONBody(status int, body map[string]interface{}) BuildOption {
	return func(ctx *PusherCtx) *PusherCtx {
		ctx.PostRequest = append(ctx.PostRequest, func(w http.ResponseWriter, r *http.Request) error {
			// Marshal the JSON body
			respBody, err := json.Marshal(body)
			if err != nil {
				return err
			}
			w.WriteHeader(status)
			w.Write(respBody)
			return nil
		})
		return ctx
	}
}

func withParserContext(fn func(http.ResponseWriter, *http.Request, context.Context) (context.Context, error)) BuildOption {
	return WithPreRequest(func(w http.ResponseWriter, r *http.Request) error {
		ctx := r.Context()
		parserCtx, err := fn(w, r, ctx) // Pass writer, request, and context to the parser function
		if err != nil {
			return err
		}
		// Update the request context with the parser context
		*r = *r.WithContext(parserCtx)
		return nil
	})
}

var withUnsnappyRequest = WithPreRequest(func(w http.ResponseWriter, r *http.Request) error {
	compressed, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	ctx := r.Context()
	uncompressed, err := func() ([]byte, error) {
		uncompressedLen, err := snappy.DecodedLen(compressed)
		if err != nil {
			return nil, err
		}
		if uncompressedLen > 10*1024*1024 {
			return nil, custom_errors.New400Error("body is too long")

		}
		uncompressed, err := snappy.Decode(nil, compressed)
		if err != nil {
			return nil, err
		}

		return uncompressed, nil
	}()
	if err != nil {
		ctx = context.WithValue(ctx, "bodyStream", bytes.NewBuffer(compressed))
		*r = *r.WithContext(ctx)
		// Sending the compressed body back
	} else {
		// Reset the request body with the uncompressed data
		ctx = context.WithValue(ctx, "bodyStream", bytes.NewBuffer(uncompressed))
		*r = *r.WithContext(ctx)
	}

	return nil
})

type readColser struct {
	io.Reader
}

func (rc readColser) Close() error { return nil }

var WithOverallContextMiddleware = WithPreRequest(func(w http.ResponseWriter, r *http.Request) error {
	dsn := strings.Clone(r.Header.Get("X-CH-DSN"))
	meta := strings.Clone(r.Header.Get("X-Scope-Meta"))
	strTTLDays := strings.Clone(r.Header.Get("X-Ttl-Days"))
	async := getAsyncMode(r)
	TTLDays := uint16(0)
	if strTTLDays != "" {
		iTTLDays, err := strconv.ParseUint(strTTLDays, 10, 16)
		if err == nil {
			TTLDays = uint16(iTTLDays)
		}
	}

	switch r.Header.Get("Content-Encoding") {
	case "":
		// No encoding, do nothing
	case "gzip":
		reader, err := gzip.NewReader(r.Body)
		if err != nil {
			return err
		}
		r.Body = readColser{reader}
	case "snappy":
		bBody, err := io.ReadAll(r.Body)
		if err != nil {
			return err
		}
		uncompressed, err := snappy.Decode(nil, bBody)
		if err != nil {
			// Sometimes senders just send uncompressed data with content-encoding: snappy
			// Complete mess, 0 out of 10
			uncompressed = bBody
		}
		reader := bytes.NewReader(uncompressed)
		r.Body = readColser{reader}
		break
	default:
		return custom_errors.New400Error(fmt.Sprintf("%s encoding not supported", r.Header.Get("Content-Encoding")))
	}
	ctx := r.Context()
	// Modify context as needed
	ctx = context.WithValue(ctx, "DSN", dsn)
	//ctx = context.WithValue(ctx, "oid", oid)
	ctx = context.WithValue(ctx, "META", meta)
	ctx = context.WithValue(ctx, "TTL_DAYS", TTLDays)
	ctx = context.WithValue(ctx, "async", async)
	//ctx = context.WithValue(ctx, "shard", shard)
	*r = *r.WithContext(ctx)
	return nil
})

var withTSAndSampleService = WithPreRequest(func(w http.ResponseWriter, r *http.Request) error {

	ctx := r.Context()
	dsn := ctx.Value("DSN")
	//// Assuming Registry functions are available and compatible with net/http
	svc, err := Registry.GetSamplesService(dsn.(string))
	if err != nil {
		return err
	}
	ctx = context.WithValue(r.Context(), "splService", svc)

	svc, err = Registry.GetTimeSeriesService(dsn.(string))
	if err != nil {
		return err
	}
	ctx = context.WithValue(ctx, "tsService", svc)

	svc, err = Registry.GetProfileInsertService(dsn.(string))
	if err != nil {
		return err
	}
	ctx = context.WithValue(ctx, "profileService", svc)

	nodeName := svc.GetNodeName()
	ctx = context.WithValue(ctx, "node", nodeName)
	*r = *r.WithContext(ctx)
	return nil
})

var withTracesService = WithPreRequest(func(w http.ResponseWriter, r *http.Request) error {
	dsn := r.Context().Value("DSN")
	svc, err := Registry.GetSpansSeriesService(dsn.(string))
	if err != nil {
		return err
	}

	ctx := context.WithValue(r.Context(), "spanAttrsService", svc)

	svc, err = Registry.GetSpansService(dsn.(string))
	if err != nil {
		return err
	}

	ctx = context.WithValue(ctx, "spansService", svc)
	ctx = context.WithValue(ctx, "node", svc.GetNodeName())
	*r = *r.WithContext(ctx)
	return nil
})
