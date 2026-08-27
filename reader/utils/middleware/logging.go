package middleware

import (
	"bufio"
	"bytes"
	"errors"
	"html"
	"net"
	"net/http"
	"text/template"
	"time"

	"github.com/metrico/qryn/v5/reader/utils/logger"
)

func LoggingMiddleware(tpl string) func(next http.Handler) http.Handler {
	t := template.New("http-logging")
	t.Parse(tpl)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// TODO: Log the request details using the template
			_w := &responseWriterWithCode{ResponseWriter: w, statusCode: http.StatusOK}
			start := time.Now()
			//t.Execute(w, r)
			next.ServeHTTP(_w, r)
			duration := time.Since(start)
			b := bytes.NewBuffer(nil)
			t.Execute(b, map[string]any{
				"method":     html.EscapeString(r.Method),
				"url":        html.EscapeString(r.URL.String()),
				"proto":      html.EscapeString(r.Proto),
				"status":     _w.statusCode,
				"length":     _w.length,
				"referer":    html.EscapeString(r.Referer()),
				"user_agent": html.EscapeString(r.UserAgent()),
				"host":       html.EscapeString(r.Host),
				"path":       html.EscapeString(r.URL.Path),
				"latency":    duration.String(),
			})
			logger.Info(b.String())
		})
	}
}

type responseWriterWithCode struct {
	http.ResponseWriter
	statusCode  int
	length      int
	wroteHeader bool
}

func (w *responseWriterWithCode) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("ResponseWriter does not support Hijack")
	}
	return h.Hijack()
}

func (w *responseWriterWithCode) WriteHeader(code int) {
	ensureSafeContentType(w.Header())
	w.statusCode = code
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(code)
}

func (w *responseWriterWithCode) Write(b []byte) (int, error) {
	ensureSafeContentType(w.Header())
	// Any Write commits the response headers (net/http sends an implicit 200).
	w.wroteHeader = true
	w.length += len(b)
	return w.ResponseWriter.Write(b)
}

// Flush forwards to the underlying writer so wrappers and handlers inside
// this middleware can stream to the client.
func (w *responseWriterWithCode) Flush() {
	f, ok := w.ResponseWriter.(http.Flusher)
	if !ok {
		return
	}
	// A flush commits the response headers (net/http sends an implicit 200).
	w.wroteHeader = true
	f.Flush()
}

// Unwrap exposes the wrapped writer to http.ResponseController passthroughs
// such as SetWriteDeadline.
func (w *responseWriterWithCode) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// HeadersSent reports whether the status line has been committed, letting
// panic handlers choose between writing an error response and aborting the
// connection.
func (w *responseWriterWithCode) HeadersSent() bool {
	return w.wroteHeader
}

// ensureSafeContentType mitigates MIME-sniffing based reflected XSS
// (go/reflected-xss). Every response funnels through the global logging and
// accept-encoding middleware wrappers, so enforcing this here guarantees that
// no response body can be sniffed and executed as HTML, regardless of which
// handler produced it. It sets X-Content-Type-Options: nosniff and, when a
// handler left Content-Type unset, defaults it to a non-HTML type.
func ensureSafeContentType(h http.Header) {
	h.Set("X-Content-Type-Options", "nosniff")
	if h.Get("Content-Type") == "" {
		h.Set("Content-Type", "text/plain; charset=utf-8")
	}
}
