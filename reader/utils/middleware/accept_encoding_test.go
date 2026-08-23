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

// assertGzipStream verifies a compressed response: complete gzip stream
// decoding to want, no Content-Length (compressed responses are streamed, so
// their size is unknown when headers are sent), and the Vary key that shared
// caches need.
func assertGzipStream(t *testing.T, w *httptest.ResponseRecorder, want string) {
	t.Helper()
	if w.Header().Get("Content-Encoding") != "gzip" {
		t.Fatalf("encoding=%q", w.Header().Get("Content-Encoding"))
	}
	if cl := w.Header().Get("Content-Length"); cl != "" {
		t.Fatalf("compressed response must not carry Content-Length, got %q", cl)
	}
	if w.Header().Get("Vary") != "Accept-Encoding" {
		t.Fatalf("Vary=%q", w.Header().Get("Vary"))
	}
	if got := gunzipAll(t, w); string(got) != want {
		t.Fatalf("body round-trip failed: got %q, want %q", got, want)
	}
}

func TestAcceptEncoding_NonEmptyBody(t *testing.T) {
	w := gzipGet(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("hello world"))
	}))
	if w.Code != 200 {
		t.Fatalf("code=%d", w.Code)
	}
	assertGzipStream(t, w, "hello world")
}

// A 2xx response whose body is a single zero-length Write must still be a
// complete gzip stream: skipping the trailer would leave clients that
// transparently decompress failing with an unexpected EOF.
func TestAcceptEncoding_EmptyWriteBody(t *testing.T) {
	w := gzipGet(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(nil)
	}))
	if w.Code != 200 {
		t.Fatalf("code=%d", w.Code)
	}
	assertGzipStream(t, w, "")
}

// A 200 with no WriteHeader and no Write at all must also decode as a valid
// empty stream.
func TestAcceptEncoding_NoWrite(t *testing.T) {
	w := gzipGet(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	if w.Code != 200 {
		t.Fatalf("code=%d", w.Code)
	}
	assertGzipStream(t, w, "")
}

// 204 must pass through untouched: RFC 9110 forbids a body (and therefore a
// Content-Encoding) on it. Every successful ingest returns 204, so this is a
// hot path, not an edge case.
func TestAcceptEncoding_NoContentPassthrough(t *testing.T) {
	w := gzipGet(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	}))
	if w.Code != 204 {
		t.Fatalf("code=%d", w.Code)
	}
	if enc := w.Header().Get("Content-Encoding"); enc != "" {
		t.Fatalf("204 must not carry Content-Encoding, got %q", enc)
	}
	if w.Body.Len() != 0 {
		t.Fatalf("204 must not carry a body, got %d bytes", w.Body.Len())
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

// 206 bodies are never compressed: the Content-Range describes the identity
// representation, and encoding the fragment would break range semantics.
func TestAcceptEncoding_PartialContentPassthrough(t *testing.T) {
	w := gzipGet(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(206)
		_, _ = w.Write([]byte("partial"))
	}))
	if w.Header().Get("Content-Encoding") == "gzip" {
		t.Fatal("206 must not be gzip encoded")
	}
	if w.Body.String() != "partial" {
		t.Fatalf("body: %q", w.Body.String())
	}
}

// TestAcceptEncoding_Negotiation covers Accept-Encoding parsing: q-values
// (RFC 9110 §12.5.3), the wildcard, case-insensitivity, and absence.
func TestAcceptEncoding_Negotiation(t *testing.T) {
	for _, c := range []struct {
		name     string
		header   string
		wantGzip bool
	}{
		{"Plain", "gzip", true},
		{"UpperCase", "GZIP", true},
		{"WithPositiveQ", "gzip;q=0.5", true},
		{"ZeroQ", "gzip;q=0", false},
		{"ZeroQWithSpace", "gzip; q=0", false},
		{"AmongOthers", "deflate, gzip;q=0.8, br", true},
		{"Wildcard", "*", true},
		{"WildcardZeroQ", "*;q=0", false},
		{"GzipZeroQBeatsWildcard", "*, gzip;q=0", false},
		{"OnlyOtherCodings", "deflate, br", false},
		{"Empty", "", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/", nil)
			if c.header != "" {
				req.Header.Set("Accept-Encoding", c.header)
			}
			w := httptest.NewRecorder()
			AcceptEncodingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte("payload"))
			})).ServeHTTP(w, req)

			gotGzip := w.Header().Get("Content-Encoding") == "gzip"
			if gotGzip != c.wantGzip {
				t.Fatalf("header %q: gzip=%v, want %v", c.header, gotGzip, c.wantGzip)
			}
			// The response varies on Accept-Encoding whichever way
			// negotiation went.
			if w.Header().Get("Vary") != "Accept-Encoding" {
				t.Fatalf("Vary=%q", w.Header().Get("Vary"))
			}
			if !gotGzip && w.Body.String() != "payload" {
				t.Fatalf("identity body: %q", w.Body.String())
			}
		})
	}
}

// HEAD responses have no body to compress; encoding metadata must not be
// fabricated for them.
func TestAcceptEncoding_HeadPassthrough(t *testing.T) {
	req := httptest.NewRequest("HEAD", "/", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	w := httptest.NewRecorder()
	AcceptEncodingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})).ServeHTTP(w, req)
	if enc := w.Header().Get("Content-Encoding"); enc != "" {
		t.Fatalf("HEAD response must not carry Content-Encoding, got %q", enc)
	}
	if w.Header().Get("Vary") != "Accept-Encoding" {
		t.Fatalf("Vary=%q", w.Header().Get("Vary"))
	}
}

// A handler that already content-encoded its body (promhttp gzips /metrics
// itself) must not be compressed a second time.
func TestAcceptEncoding_AlreadyEncodedPassthrough(t *testing.T) {
	w := gzipGet(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write([]byte("pre-compressed bytes"))
	}))
	if w.Body.String() != "pre-compressed bytes" {
		t.Fatalf("body was re-encoded: %q", w.Body.String())
	}
}

// A handler panic must not produce a complete-looking response: the stream is
// left without its gzip trailer so the client's decoder reports truncation.
func TestAcceptEncoding_PanicLeavesTruncatedStream(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	w := httptest.NewRecorder()
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected the handler panic to propagate")
			}
		}()
		AcceptEncodingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("partial result"))
			panic("handler crashed")
		})).ServeHTTP(w, req)
	}()

	zr, err := gzip.NewReader(w.Body)
	if err != nil {
		return
	}
	if _, err := io.ReadAll(zr); err == nil {
		t.Fatal("aborted response decoded as a complete gzip stream")
	}
}

// Flush must reach the client: the gzip layer emits a sync block so bytes
// written before the flush decode on their own, and the transport is flushed.
func TestAcceptEncoding_FlushStreams(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	w := httptest.NewRecorder()

	AcceptEncodingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("hello"))
		w.(http.Flusher).Flush()
	})).ServeHTTP(w, req)

	if !w.Flushed {
		t.Fatal("Flush did not reach the underlying ResponseWriter")
	}
	assertGzipStream(t, w, "hello")
}

// The wrapper must keep http.ResponseController working via Unwrap, and
// expose Flusher directly.
func TestAcceptEncoding_WriterInterfaces(t *testing.T) {
	gzipGet(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := w.(http.Flusher); !ok {
			t.Error("wrapper does not implement http.Flusher")
		}
		if _, ok := w.(interface{ Unwrap() http.ResponseWriter }); !ok {
			t.Error("wrapper does not implement Unwrap for http.ResponseController")
		}
	}))
}
