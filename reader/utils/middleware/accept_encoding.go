package middleware

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"
)

func AcceptEncodingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			gzw := newGzipResponseWriter(w)
			defer gzw.Close()
			next.ServeHTTP(gzw, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// gzipResponseWriter wraps the http.ResponseWriter to provide gzip functionality
type gzipResponseWriter struct {
	http.ResponseWriter
	Writer    *gzip.Writer
	code      int
	codeSet   bool
	preBuffer bytes.Buffer
}

func newGzipResponseWriter(w http.ResponseWriter) *gzipResponseWriter {
	res := &gzipResponseWriter{
		ResponseWriter: w,
		code:           200,
	}
	gz := gzip.NewWriter(&res.preBuffer)
	res.Writer = gz
	return res
}

func (gzw *gzipResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := gzw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("ResponseWriter does not support Hijack")
	}
	return h.Hijack()
}

func (gzw *gzipResponseWriter) WriteHeader(code int) {
	if gzw.codeSet {
		return
	}
	gzw.codeSet = true
	gzw.code = code
	ensureSafeContentType(gzw.Header())
	if gzw.code/100 == 2 {
		gzw.Header().Set("Content-Encoding", "gzip")
	} else {
		gzw.ResponseWriter.WriteHeader(code)
	}

}

func (gzw *gzipResponseWriter) Write(b []byte) (int, error) {
	gzw.codeSet = true
	if gzw.code/100 == 2 {
		gzw.Header().Set("Content-Encoding", "gzip")
		return gzw.Writer.Write(b)
	}
	return gzw.ResponseWriter.Write(b)
}

func (gzw *gzipResponseWriter) Close() {
	if gzw.code/100 != 2 {
		return
	}
	// Always terminate the gzip stream: a first Write emits the gzip header
	// into preBuffer even for zero-length bodies, and without the Close
	// trailer the response would be a truncated stream that clients fail to
	// decode. Closing with no writes at all produces a valid empty stream.
	gzw.Writer.Close()
	// Covers the implicit-200 path where the handler wrote a body without ever
	// calling WriteHeader: headers are only flushed here in Close.
	ensureSafeContentType(gzw.Header())
	gzw.Header().Set("Content-Length", strconv.Itoa(gzw.preBuffer.Len()))
	gzw.ResponseWriter.WriteHeader(gzw.code)
	gzw.ResponseWriter.Write(gzw.preBuffer.Bytes())
}
