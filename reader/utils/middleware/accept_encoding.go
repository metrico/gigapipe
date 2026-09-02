package middleware

import (
	"bufio"
	"compress/gzip"
	"errors"
	"math"
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
// Partial content and bodies a handler already content-encoded pass through
// untouched. Statuses that must not carry a body (1xx, 204, 205, 304) are
// never compressed, and body writes on them are refused with
// http.ErrBodyNotAllowed.
func AcceptEncodingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The representation varies on Accept-Encoding no matter what this
		// particular request negotiated; shared caches must key on it.
		// Add, not Set, so a handler-supplied Vary member survives; handlers
		// in turn must Add rather than Set to preserve this one.
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
	var gzipQ, wildcardQ float64
	var gzipSeen, wildcardSeen bool
	for _, headerValue := range h.Values("Accept-Encoding") {
		for element := range strings.SplitSeq(headerValue, ",") {
			coding, params, _ := strings.Cut(element, ";")
			coding = strings.TrimSpace(coding)
			q := qValue(params)
			switch {
			// Content-coding names are case-insensitive (RFC 9110 §8.4.1).
			case strings.EqualFold(coding, "gzip"):
				gzipQ, gzipSeen = q, true
			case coding == "*":
				wildcardQ, wildcardSeen = q, true
			}
		}
	}
	// An explicitly listed gzip always wins over the wildcard, whatever its
	// weight.
	if gzipSeen {
		return gzipQ > 0
	}
	return wildcardSeen && wildcardQ > 0
}

// qValue extracts the q parameter from a header element's parameter list.
// RFC 9110 §12.4.2 restricts qvalue to [0, 1]; parseable weights outside that
// range are clamped to it, preserving the direction the client expressed,
// while absent or unparseable (including NaN/Inf) weights default to 1.
func qValue(params string) float64 {
	for param := range strings.SplitSeq(params, ";") {
		name, value, ok := strings.Cut(param, "=")
		if !ok || !strings.EqualFold(strings.TrimSpace(name), "q") {
			continue
		}
		q, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil || math.IsNaN(q) || math.IsInf(q, 0) {
			return 1
		}
		return math.Min(math.Max(q, 0), 1)
	}
	return 1
}

// bodyForbidden reports whether the given status code must not carry a message body
// per RFC 9110 §6.3 and §15.
func bodyForbidden(code int) bool {
	return code == http.StatusNoContent ||
		code == http.StatusResetContent ||
		code == http.StatusNotModified ||
		(code >= 100 && code < 200)
}

// gzipResponseWriter wraps the http.ResponseWriter to provide gzip functionality
type gzipResponseWriter struct {
	http.ResponseWriter
	gz       *gzip.Writer
	status   int
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
	gzw.status = code
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
	// net/http itself refuses body writes on 1xx/204/304; 205 forbids content
	// just the same (RFC 9110 §15.3.6), so refuse it identically and handlers
	// see one consistent error across every body-forbidden status.
	if bodyForbidden(gzw.status) {
		return 0, http.ErrBodyNotAllowed
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
			// A failed write almost always means the client went away
			// mid-response, which is routine on a query service.
			logger.Debug("gzip middleware: flush error: ", err)
		}
	}
	// ResponseController reaches the transport through wrappers that expose
	// either Flush or Unwrap.
	_ = http.NewResponseController(gzw.ResponseWriter).Flush()
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

// HeadersSent reports whether the status line has been committed, letting
// panic handlers choose between writing an error response and aborting the
// connection.
func (gzw *gzipResponseWriter) HeadersSent() bool {
	return gzw.decided
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
		// A failed write almost always means the client went away
		// mid-response, which is routine on a query service.
		logger.Debug("gzip middleware: close error: ", err)
	}
}
