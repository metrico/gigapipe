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

	"github.com/metrico/qryn/reader/utils/logger"
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
	statusCode int
	length     int
}

func (w *responseWriterWithCode) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("ResponseWriter does not support Hijack")
	}
	return h.Hijack()
}

func (w *responseWriterWithCode) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *responseWriterWithCode) Write(b []byte) (int, error) {
	w.length += len(b)
	return w.ResponseWriter.Write(b)
}
