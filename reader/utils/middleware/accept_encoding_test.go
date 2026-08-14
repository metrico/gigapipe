package middleware

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func gzipGet(t *testing.T, handler http.Handler) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	w := httptest.NewRecorder()
	AcceptEncodingMiddleware(handler).ServeHTTP(w, req)
	return w
}

func gunzipAll(t *testing.T, w *httptest.ResponseRecorder) []byte {
	t.Helper()
	zr, err := gzip.NewReader(w.Body)
	if err != nil {
		t.Fatalf("response is not a valid gzip stream: %v", err)
	}
	out, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("gzip stream is truncated or corrupt: %v", err)
	}
	return out
}

func TestAcceptEncoding_NonEmptyBody(t *testing.T) {
	w := gzipGet(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("hello world"))
	}))
	if w.Code != 200 || w.Header().Get("Content-Encoding") != "gzip" {
		t.Fatalf("code=%d encoding=%q", w.Code, w.Header().Get("Content-Encoding"))
	}
	if got := gunzipAll(t, w); string(got) != "hello world" {
		t.Fatalf("body round-trip failed: %q", got)
	}
}

// A 2xx response whose body is a single zero-length Write must still be a
// complete gzip stream: the first Write emits the gzip header, and skipping
// the trailer would leave clients that transparently decompress failing with
// an unexpected EOF.
func TestAcceptEncoding_EmptyWriteBody(t *testing.T) {
	w := gzipGet(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(nil)
	}))
	if w.Code != 200 || w.Header().Get("Content-Encoding") != "gzip" {
		t.Fatalf("code=%d encoding=%q", w.Code, w.Header().Get("Content-Encoding"))
	}
	if got := gunzipAll(t, w); len(got) != 0 {
		t.Fatalf("expected empty decoded body, got %q", got)
	}
}

// A 2xx response with no Write call at all must also decode as an empty
// stream.
func TestAcceptEncoding_NoWrite(t *testing.T) {
	w := gzipGet(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	}))
	if w.Code != 204 {
		t.Fatalf("code=%d", w.Code)
	}
	if got := gunzipAll(t, w); len(got) != 0 {
		t.Fatalf("expected empty decoded body, got %q", got)
	}
}

func TestAcceptEncoding_ErrorPassthrough(t *testing.T) {
	w := gzipGet(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		_, _ = w.Write([]byte("bad request"))
	}))
	if w.Code != 400 {
		t.Fatalf("code=%d", w.Code)
	}
	if w.Header().Get("Content-Encoding") == "gzip" {
		t.Fatal("non-2xx responses must not be gzip encoded")
	}
	if w.Body.String() != "bad request" {
		t.Fatalf("body: %q", w.Body.String())
	}
}
