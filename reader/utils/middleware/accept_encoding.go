package middleware

import (
	"bufio"
	"compress/gzip"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/metrico/qryn/v5/reader/utils/logger"
)

// AcceptEncodingMiddleware gzips 2xx response bodies for clients whose
// Accept-Encoding header allows it, streaming compressed bytes to the client
// as the handler writes. Compressed responses carry no Content-Length (the
// compressed size is unknown up front), so they are sent chunked.
// Responses that must not carry a body (204, 304, 1xx), partial content, and
// bodies a handler already content-encoded pass through untouched.
func AcceptEncodingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The representation varies on Accept-Encoding no matter what this
		// particular request negotiated; shared caches must key on it.
		w.Header().Add("Vary", "Accept-Encoding")
		// A HEAD response carries no body, so there is nothing to compress;
		// wrapping would only fabricate Content-Encoding metadata that
		// disagrees with the corresponding GET.
		if r.Method == http.MethodHead || !acceptsGzip(r.Header) {
			next.ServeHTTP(w, r)
			return
		}
		gzw := newGzipResponseWriter(w)
		next.ServeHTTP(gzw, r)
		// Straight-line, not deferred: when the handler panics the stream is
		// left without its gzip trailer, so clients detect the truncation
		// instead of successfully decoding a partial body.
		gzw.finish()
	})
}

// acceptsGzip reports whether the request's Accept-Encoding header permits a
// gzip response, honoring q-values (RFC 9110 §12.5.3): "gzip;q=0" forbids
// gzip, and a wildcard element applies when gzip is not explicitly listed.
// An absent header means no compression.
func acceptsGzip(h http.Header) bool {
	gzipQ, wildcardQ := -1.0, -1.0
	for _, headerValue := range h.Values("Accept-Encoding") {
		for _, element := range strings.Split(headerValue, ",") {
			coding, params, _ := strings.Cut(element, ";")
			coding = strings.TrimSpace(coding)
			q := qValue(params)
			switch {
			// Content-coding names are case-insensitive (RFC 9110 §8.4.1).
			case strings.EqualFold(coding, "gzip"):
				gzipQ = q
			case coding == "*":
				wildcardQ = q
			}
		}
	}
	if gzipQ >= 0 {
		return gzipQ > 0
	}
	return wildcardQ > 0
}

// qValue extracts the q parameter from a header element's parameter list,
// defaulting to 1 when absent or malformed.
func qValue(params string) float64 {
	for _, param := range strings.Split(params, ";") {
		name, value, ok := strings.Cut(param, "=")
		if !ok || !strings.EqualFold(strings.TrimSpace(name), "q") {
			continue
		}
		q, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil {
			return 1
		}
		return q
	}
	return 1
}

// bodyForbidden reports whether the given status code must not carry a message body
// per RFC 9110 §6.3 and §15.
func bodyForbidden(code int) bool {
	return code == http.StatusNoContent ||
		code == http.StatusNotModified ||
		(code >= 100 && code < 200)
}

// gzipResponseWriter wraps the http.ResponseWriter to provide gzip functionality
type gzipResponseWriter struct {
	http.ResponseWriter
	gz       *gzip.Writer
	compress bool
	decided  bool
	hijacked bool
}

func newGzipResponseWriter(w http.ResponseWriter) *gzipResponseWriter {
	return &gzipResponseWriter{ResponseWriter: w}
}

// decide fixes, at header-commit time, whether the body will be compressed,
// and forwards the status line. Compression applies only to 2xx responses
// that may carry a body, excluding partial content (a Content-Range describes
// the identity representation) and bodies the handler already
// content-encoded itself (e.g. promhttp performs its own gzip negotiation).
func (gzw *gzipResponseWriter) decide(code int) {
	gzw.decided = true
	gzw.compress = code/100 == 2 &&
		!bodyForbidden(code) &&
		code != http.StatusPartialContent &&
		gzw.Header().Get("Content-Encoding") == ""
	if !bodyForbidden(code) {
		ensureSafeContentType(gzw.Header())
	}
	if gzw.compress {
		gzw.Header().Set("Content-Encoding", "gzip")
		// Any Content-Length computed upstream describes the uncompressed
		// body; the compressed size is unknown, so the response is chunked.
		gzw.Header().Del("Content-Length")
		gzw.gz = gzip.NewWriter(gzw.ResponseWriter)
	}
	gzw.ResponseWriter.WriteHeader(code)
}

func (gzw *gzipResponseWriter) WriteHeader(code int) {
	// Interim (1xx) responses precede the final status; forward them without
	// latching the compression decision.
	if code >= 100 && code < 200 {
		gzw.ResponseWriter.WriteHeader(code)
		return
	}
	if gzw.decided {
		return
	}
	gzw.decide(code)
}

func (gzw *gzipResponseWriter) Write(b []byte) (int, error) {
	if !gzw.decided {
		gzw.decide(http.StatusOK)
	}
	if gzw.compress {
		return gzw.gz.Write(b)
	}
	return gzw.ResponseWriter.Write(b)
}

// Flush pushes everything written so far to the client: the gzip layer emits
// a sync block so the bytes sent decode on their own, then the transport is
// flushed.
func (gzw *gzipResponseWriter) Flush() {
	if gzw.hijacked {
		return
	}
	if !gzw.decided {
		gzw.decide(http.StatusOK)
	}
	if gzw.compress {
		if err := gzw.gz.Flush(); err != nil {
			logger.Error("gzip middleware: flush error: ", err)
			return
		}
	}
	if f, ok := gzw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (gzw *gzipResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := gzw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("ResponseWriter does not support Hijack")
	}
	conn, rw, err := h.Hijack()
	if err == nil {
		// The connection now belongs to the handler (e.g. a WebSocket
		// upgrade on /loki/api/v1/tail); finish must not touch the response.
		gzw.hijacked = true
	}
	return conn, rw, err
}

// Unwrap exposes the wrapped writer to http.ResponseController passthroughs
// such as SetWriteDeadline; Flush and Hijack are implemented on the wrapper
// itself and take precedence.
func (gzw *gzipResponseWriter) Unwrap() http.ResponseWriter {
	return gzw.ResponseWriter
}

// finish terminates the gzip stream, emitting the trailer that lets clients
// verify they received the whole body. A response with no writes at all still
// becomes a valid empty stream.
func (gzw *gzipResponseWriter) finish() {
	if gzw.hijacked {
		return
	}
	if !gzw.decided {
		gzw.decide(http.StatusOK)
	}
	if !gzw.compress {
		return
	}
	if err := gzw.gz.Close(); err != nil {
		logger.Error("gzip middleware: close error: ", err)
	}
}
