package middleware

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const reflectedPayload = "<script>alert(1)</script>"

// assertNotSniffableAsHTML checks the two guarantees that defuse MIME-sniffing
// based reflected XSS (go/reflected-xss): the response must carry
// X-Content-Type-Options: nosniff and must not declare an HTML Content-Type.
func assertNotSniffableAsHTML(t *testing.T, h http.Header) {
	t.Helper()
	if got := h.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q, want %q", got, "nosniff")
	}
	ct := h.Get("Content-Type")
	if ct == "" {
		t.Errorf("Content-Type is unset; response could be sniffed as HTML")
	}
	if strings.Contains(strings.ToLower(ct), "text/html") {
		t.Errorf("Content-Type = %q declares HTML; reflected input becomes executable", ct)
	}
}

// reflectingHandler writes attacker-controlled input (the "q" query param)
// straight into the response body without setting a Content-Type, i.e. the
// exact shape CodeQL flags as a reflected-XSS source.
func reflectingHandler(status int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if status != http.StatusOK {
			w.WriteHeader(status)
		}
		_, _ = w.Write([]byte(r.URL.Query().Get("q")))
	}
}

func newReflectingRequest() *http.Request {
	return httptest.NewRequest(http.MethodGet, "/?q="+reflectedPayload, nil)
}

func TestLoggingMiddleware_NotSniffableAsHTML(t *testing.T) {
	h := LoggingMiddleware("{{.status}}")(reflectingHandler(http.StatusOK))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, newReflectingRequest())

	assertNotSniffableAsHTML(t, rec.Result().Header)
	if !strings.Contains(rec.Body.String(), reflectedPayload) {
		t.Fatalf("test handler did not reflect the payload; body=%q", rec.Body.String())
	}
}

func TestAcceptEncodingMiddleware_NonGzip_NotSniffableAsHTML(t *testing.T) {
	// Non-2xx exercises the plain (non-gzip) write passthrough that CodeQL
	// flags at accept_encoding.go Write.
	h := AcceptEncodingMiddleware(reflectingHandler(http.StatusBadRequest))
	rec := httptest.NewRecorder()
	req := newReflectingRequest()
	req.Header.Set("Accept-Encoding", "gzip")
	h.ServeHTTP(rec, req)

	assertNotSniffableAsHTML(t, rec.Result().Header)
}

func TestAcceptEncodingMiddleware_Gzip2xx_NotSniffableAsHTML(t *testing.T) {
	// 2xx buffers through gzip and flushes headers in Close.
	h := AcceptEncodingMiddleware(reflectingHandler(http.StatusOK))
	rec := httptest.NewRecorder()
	req := newReflectingRequest()
	req.Header.Set("Accept-Encoding", "gzip")
	h.ServeHTTP(rec, req)

	res := rec.Result()
	assertNotSniffableAsHTML(t, res.Header)

	// Sanity: the reflected payload is really in the (gzip) body.
	if res.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(res.Body)
		if err != nil {
			t.Fatalf("gzip.NewReader: %v", err)
		}
		body, _ := io.ReadAll(zr)
		if !strings.Contains(string(body), reflectedPayload) {
			t.Fatalf("gzip body did not reflect payload; got %q", string(body))
		}
	}
}

// TestEnsureSafeContentType_PreservesExplicitType guards against regressions
// that would clobber a handler's own Content-Type (e.g. application/json).
func TestEnsureSafeContentType_PreservesExplicitType(t *testing.T) {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	ensureSafeContentType(h)

	if got := h.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want it preserved as application/json", got)
	}
	if got := h.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q, want nosniff", got)
	}
}
