package controller

import (
	"fmt"
	"io"
	"net/http"

	"github.com/metrico/qryn/v5/writer/config"
	"github.com/metrico/qryn/v5/writer/utils/logger"
	"github.com/metrico/qryn/v5/writer/utils/unmarshal"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	otlpContentTypeProto = "application/x-protobuf"
	otlpContentTypeJSON  = "application/json"
)

// defaultOTLPMaxMessageSize is 64 MiB, the OTLP/HTTP spec's recommended
// request-size bound.
const defaultOTLPMaxMessageSize = 64 << 20

// OTLPMaxMessageSize returns the maximum decompressed OTLP payload size in
// bytes, shared by both transports: the OTLP/HTTP request body limit
// (oversize requests get HTTP 413) and the gRPC server's MaxRecvMsgSize. One
// value for both, so a batch accepted on one transport is never rejected on
// the other. Configured via system_settings.otlp_max_message_size (env:
// QRYN_SYSTEM_SETTINGS_OTLP_MAX_MESSAGE_SIZE); non-positive values and a
// missing config both fall back to the 64 MiB default.
func OTLPMaxMessageSize() int {
	if config.Cloki != nil && config.Cloki.Setting != nil {
		if v := config.Cloki.Setting.SYSTEM_SETTINGS.OTLPMaxMessageSize; v > 0 {
			return v
		}
	}
	return defaultOTLPMaxMessageSize
}

// OTLPMetricsV2 handles OTLP/HTTP metrics export on /v1/metrics. It accepts
// binary protobuf and JSON payloads, dispatched on Content-Type, and responds
// per the OTLP/HTTP spec: 200 with an ExportMetricsServiceResponse in the
// request's encoding (partial_success set when data points were rejected),
// 400 with a google.rpc.Status for undecodable payloads, 413 for oversize
// bodies, and 503 with a google.rpc.Status for transient ingest failures.
func OTLPMetricsV2(cfg MiddlewareConfig) func(w http.ResponseWriter, r *http.Request) {
	return Build(
		append(cfg.ExtraMiddleware,
			withTSAndSampleService,
			withOTLPMetricsParser(otlpContentTypeProto, func(body []byte, md *metricsv1.MetricsData) error {
				return proto.Unmarshal(body, md)
			}),
			withOTLPMetricsParser(otlpContentTypeJSON, func(body []byte, md *metricsv1.MetricsData) error {
				return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(body, md)
			}),
		)...,
	)
}

func withOTLPMetricsParser(contentType string, decode func([]byte, *metricsv1.MetricsData) error) BuildOption {
	return func(ctx *PusherCtx) *PusherCtx {
		ctx.Parser[contentType] = func(w http.ResponseWriter, r *http.Request) error {
			return serveOTLPMetrics(w, r, contentType, decode)
		}
		return ctx
	}
}

func serveOTLPMetrics(w http.ResponseWriter, r *http.Request, contentType string,
	decode func([]byte, *metricsv1.MetricsData) error) error {
	maxSize := OTLPMaxMessageSize()
	body, err := io.ReadAll(io.LimitReader(getBodyStream(r), int64(maxSize)+1))
	if err != nil {
		return writeOTLPStatus(w, contentType, http.StatusBadRequest,
			fmt.Sprintf("failed to read request body: %v", err))
	}
	if len(body) > maxSize {
		return writeOTLPStatus(w, contentType, http.StatusRequestEntityTooLarge,
			fmt.Sprintf("request body exceeds the %d byte limit", maxSize))
	}

	md := &metricsv1.MetricsData{}
	if err := decode(body, md); err != nil {
		return writeOTLPStatus(w, contentType, http.StatusBadRequest,
			fmt.Sprintf("failed to decode metrics payload: %v", err))
	}

	stats := &unmarshal.OTLPMetricsStats{}
	if err := doParse(r, Parser(unmarshal.OTLPMetricsFromData(md, stats))); err != nil {
		logger.Error("OTLP metrics ingest failed: ", err)
		return writeOTLPStatus(w, contentType, http.StatusServiceUnavailable, "temporary ingestion failure")
	}

	resp := &colmetricspb.ExportMetricsServiceResponse{}
	if rejected := stats.RejectedDataPoints(); rejected > 0 {
		resp.PartialSuccess = &colmetricspb.ExportMetricsPartialSuccess{
			RejectedDataPoints: rejected,
			ErrorMessage:       stats.ErrorMessage(),
		}
	}
	return writeOTLPMessage(w, contentType, http.StatusOK, resp)
}

// writeOTLPStatus responds with a google.rpc.Status encoded in the request's
// content type, as the OTLP/HTTP spec requires for all non-200 responses.
func writeOTLPStatus(w http.ResponseWriter, contentType string, httpCode int, message string) error {
	return writeOTLPMessage(w, contentType, httpCode, &spb.Status{Message: message})
}

func writeOTLPMessage(w http.ResponseWriter, contentType string, httpCode int, msg proto.Message) error {
	var payload []byte
	var err error
	if contentType == otlpContentTypeJSON {
		payload, err = protojson.Marshal(msg)
	} else {
		payload, err = proto.Marshal(msg)
	}
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(httpCode)
	_, _ = w.Write(payload)
	return nil
}
