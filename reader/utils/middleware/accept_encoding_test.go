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

// 205 responses cannot contain content (RFC 9110 §15.3.6); the middleware
// must not wrap them in a gzip envelope.
func TestAcceptEncoding_ResetContentPassthrough(t *testing.T) {
	w := gzipGet(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(205)
	}))
	if w.Code != 205 {
		t.Fatalf("code=%d", w.Code)
	}
	if enc := w.Header().Get("Content-Encoding"); enc != "" {
		t.Fatalf("205 must not carry Content-Encoding, got %q", enc)
	}
	if w.Body.Len() != 0 {
		t.Fatalf("205 must not carry content, got %d bytes", w.Body.Len())
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
		// Weights outside [0,1] are clamped; the client's direction is kept.
		{"NegativeQ", "gzip;q=-1", false},
		{"NegativeQBeatsWildcard", "*, gzip;q=-1", false},
		{"OutOfRangeQ", "gzip;q=5", true},
		// Unparseable weights fall back to the default q=1.
		{"NaNQ", "gzip;q=NaN", true},
		{"InfQ", "gzip;q=Inf", true},
		{"GarbageQ", "gzip;q=abc", true},
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

type flushRecorder struct {
	*httptest.ResponseRecorder
	flushed bool
}

func (f *flushRecorder) Flush() { f.flushed = true; f.ResponseRecorder.Flush() }

// TestAcceptEncoding_ProdChainFlush builds the middleware chain exactly as
// cmd/gigapipe/main.go does (Logging outermost, then Cors, then
// AcceptEncoding) and asserts a handler Flush reaches the transport through
// every wrapper in between.
func TestAcceptEncoding_ProdChainFlush(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("chunk"))
		f, ok := w.(http.Flusher)
		if !ok {
			t.Fatalf("handler writer %T is not an http.Flusher", w)
		}
		f.Flush()
	})
	h := LoggingMiddleware("chain-test")(CorsMiddleware("*")(AcceptEncodingMiddleware(inner)))

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	fr := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	h.ServeHTTP(fr, req)

	if !fr.flushed {
		t.Fatal("handler Flush did not reach the transport through the full middleware chain")
	}
	assertGzipStream(t, fr.ResponseRecorder, "chunk")
}

// The logging wrapper sits between the gzip layer and the transport; it must
// forward Flush and expose Unwrap so http.ResponseController keeps working,
// and report header commitment for panic handling.
func TestLogging_WriterInterfaces(t *testing.T) {
	var rw http.ResponseWriter = &responseWriterWithCode{ResponseWriter: httptest.NewRecorder()}
	if _, ok := rw.(http.Flusher); !ok {
		t.Error("responseWriterWithCode does not implement http.Flusher")
	}
	if _, ok := rw.(interface{ Unwrap() http.ResponseWriter }); !ok {
		t.Error("responseWriterWithCode does not implement Unwrap for http.ResponseController")
	}
	hs, ok := rw.(interface{ HeadersSent() bool })
	if !ok {
		t.Fatal("responseWriterWithCode does not report HeadersSent")
	}
	if hs.HeadersSent() {
		t.Error("HeadersSent=true before any write")
	}
	_, _ = rw.Write([]byte("x"))
	if !hs.HeadersSent() {
		t.Error("HeadersSent=false after a body write")
	}
}

// plainWriter is an http.ResponseWriter that is not an http.Flusher.
type plainWriter struct{ header http.Header }

func (p plainWriter) Header() http.Header         { return p.header }
func (p plainWriter) Write(b []byte) (int, error) { return len(b), nil }
func (p plainWriter) WriteHeader(int)             {}

// A bare Flush commits the response — net/http sends an implicit 200 — so the
// wrapper must report the headers as sent afterwards; otherwise a panic
// following the flush would be answered with a 500 written onto an
// already-committed 200. When the underlying writer is not a Flusher, nothing
// is committed and the flag must stay false.
func TestLogging_FlushCommitsHeaders(t *testing.T) {
	var rw http.ResponseWriter = &responseWriterWithCode{ResponseWriter: httptest.NewRecorder()}
	hs := rw.(interface{ HeadersSent() bool })
	if hs.HeadersSent() {
		t.Error("HeadersSent=true before any write")
	}
	rw.(http.Flusher).Flush()
	if !hs.HeadersSent() {
		t.Error("HeadersSent=false after Flush committed the response")
	}

	var noFlush http.ResponseWriter = &responseWriterWithCode{ResponseWriter: plainWriter{header: http.Header{}}}
	noFlush.(http.Flusher).Flush()
	if noFlush.(interface{ HeadersSent() bool }).HeadersSent() {
		t.Error("HeadersSent=true although the underlying writer cannot flush")
	}
}

// A body written after a body-forbidden status must not reach the client.
// net/http drops 1xx/204/304 bodies itself but ships a 205 body; the gzip
// wrapper refuses all of them with http.ErrBodyNotAllowed so every
// body-forbidden status behaves the same. Asserted against a real server —
// httptest.ResponseRecorder does not model transport framing.
func TestAcceptEncoding_BodyForbiddenStatusesDropBody(t *testing.T) {
	for _, code := range []int{204, 205, 304} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			var writeErr error
			srv := httptest.NewServer(LoggingMiddleware("chain-test")(CorsMiddleware("*")(
				AcceptEncodingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(code)
					_, writeErr = w.Write([]byte("this must not be sent"))
				})))))
			defer srv.Close()

			req, err := http.NewRequest("GET", srv.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Accept-Encoding", "gzip")
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != code {
				t.Fatalf("code=%d, want %d", resp.StatusCode, code)
			}
			if enc := resp.Header.Get("Content-Encoding"); enc != "" {
				t.Fatalf("body-forbidden status must not carry Content-Encoding, got %q", enc)
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if len(body) != 0 {
				t.Fatalf("client received %d body bytes: %q", len(body), body)
			}
			if writeErr != http.ErrBodyNotAllowed {
				t.Fatalf("handler write error = %v, want http.ErrBodyNotAllowed", writeErr)
			}
		})
	}
}

// HeadersSent lets panic handlers distinguish "safe to write a 500" from
// "status line already on the wire".
func TestAcceptEncoding_HeadersSent(t *testing.T) {
	gzipGet(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hs, ok := w.(interface{ HeadersSent() bool })
		if !ok {
			t.Fatal("gzip wrapper does not report HeadersSent")
		}
		if hs.HeadersSent() {
			t.Error("HeadersSent=true before any write")
		}
		w.WriteHeader(200)
		if !hs.HeadersSent() {
			t.Error("HeadersSent=false after WriteHeader")
		}
	}))
}
