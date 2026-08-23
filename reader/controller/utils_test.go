package controller

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/utils/middleware"
)

func TestParseTimeSecOrRFCMagnitudes(t *testing.T) {
	def := time.Unix(0, 0)
	cases := []struct {
		raw  string
		want time.Time
	}{
		{"1785416892", time.Unix(1785416892, 0)},                                // seconds
		{"1785416892.5", time.Unix(1785416892, 500000000)},                      // fractional seconds
		{"1785416892000", time.Unix(1785416892, 0)},                             // milliseconds
		{"1785416892123", time.Unix(1785416892, 123000000)},                     // milliseconds, sub-second
		{"1785416892000000", time.Unix(1785416892, 0)},                          // microseconds
		{"1785416892123456", time.Unix(1785416892, 123456000)},                  // microseconds, sub-second
		{"1785416892000000000", time.Unix(0, 1785416892000000000)},              // nanoseconds
		{"1785416892123456789", time.Unix(0, 1785416892123456789)},              // nanoseconds, exact
		{"2026-07-30T13:00:00Z", time.Date(2026, 7, 30, 13, 0, 0, 0, time.UTC)}, // RFC3339
	}
	for _, c := range cases {
		got, err := ParseTimeSecOrRFC(c.raw, def)
		if err != nil {
			t.Fatalf("%s: %v", c.raw, err)
		}
		if !got.Equal(c.want) {
			t.Errorf("%s: got %v want %v", c.raw, got.UTC(), c.want.UTC())
		}
	}
	got, _ := ParseTimeSecOrRFC("", def)
	if !got.Equal(def) {
		t.Errorf("empty: got %v", got)
	}
}

// epochToTime is shared with the Tempo query path, which previously used a
// coarser variant that misread microsecond epochs as milliseconds and
// nanosecond epochs below 1e18 as milliseconds.
func TestEpochToTimeSharedWithTempo(t *testing.T) {
	cases := []struct {
		in   int64
		want time.Time
	}{
		{1785416892, time.Unix(1785416892, 0)},
		{1785416892123, time.Unix(1785416892, 123000000)},
		{1785416892123456, time.Unix(1785416892, 123456000)},
		{1785416892123456789, time.Unix(0, 1785416892123456789)},
		{999999999123456789, time.Unix(0, 999999999123456789)}, // ns epoch < 1e18
		{0, time.Unix(0, 0)},
	}
	for _, c := range cases {
		if got := epochToTime(c.in, 0); !got.Equal(c.want) {
			t.Errorf("%d: got %v want %v", c.in, got.UTC(), c.want.UTC())
		}
	}
}

// A panic before anything is written must surface as a plain 500.
func TestTamePanic_BeforeWrite(t *testing.T) {
	h := middleware.AcceptEncodingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer tamePanic(w, r)
		panic("boom")
	}))
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != 500 {
		t.Fatalf("code=%d, want 500", w.Code)
	}
	if w.Header().Get("Content-Encoding") != "" {
		t.Fatalf("error response must not be encoded, got %q", w.Header().Get("Content-Encoding"))
	}
	if w.Body.String() != "Internal Server Error" {
		t.Fatalf("body=%q", w.Body.String())
	}
}

// A panic after the status line is committed must not produce a
// complete-looking success response: tamePanic aborts the connection via
// http.ErrAbortHandler, leaving the gzip stream without its trailer so
// clients detect the truncation.
func TestTamePanic_MidBodyAbortsCompressedStream(t *testing.T) {
	h := middleware.AcceptEncodingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer tamePanic(w, r)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"status":"success","data":`))
		panic("boom")
	}))
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	w := httptest.NewRecorder()

	func() {
		defer func() {
			if rec := recover(); rec != http.ErrAbortHandler {
				t.Fatalf("recovered %v, want http.ErrAbortHandler", rec)
			}
		}()
		h.ServeHTTP(w, req)
	}()

	zr, err := gzip.NewReader(w.Body)
	if err != nil {
		return
	}
	if body, err := io.ReadAll(zr); err == nil {
		t.Fatalf("aborted response decoded as a complete gzip stream: %q", body)
	}
}
