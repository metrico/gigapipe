package middleware

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"
)

const (
	testLogin = "user"
	testPass  = "s3cret"
)

// serveWithAuth runs one request carrying the given Authorization header
// (omitted entirely when authHeader is "") through BasicAuthMiddleware, and
// reports the response plus whether the wrapped handler was reached.
func serveWithAuth(t *testing.T, authHeader string) (*httptest.ResponseRecorder, bool) {
	t.Helper()
	nextCalled := false
	h := BasicAuthMiddleware(testLogin, testPass)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextCalled = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec, nextCalled
}

func basicHeader(login, pass string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(login+":"+pass))
}

// TestBasicAuth_Rejects covers every rejection path. Each case asserts the
// status code, that a WWW-Authenticate challenge was sent (RFC 7235 §4.1
// requires one on every 401), AND that the wrapped handler never ran -- a
// status assertion alone would not prove the request was actually blocked.
func TestBasicAuth_Rejects(t *testing.T) {
	for _, c := range []struct {
		name       string
		authHeader string
	}{
		{"MissingHeader", ""},
		{"NoScheme", "abc"},
		{"BearerScheme", "Bearer " + base64.StdEncoding.EncodeToString([]byte("user:s3cret"))},
		{"NonBase64Payload", "Basic !!!!"},
		{"NoColonInPayload", "Basic " + base64.StdEncoding.EncodeToString([]byte("usernopass"))},
		{"WrongLogin", basicHeader("wrong", testPass)},
		{"WrongPass", basicHeader(testLogin, "wrong")},
		{"EmptyCredentials", basicHeader("", "")},
		// A prefix of the real password must not be accepted.
		{"PassPrefix", basicHeader(testLogin, "s3cre")},
	} {
		t.Run(c.name, func(t *testing.T) {
			rec, nextCalled := serveWithAuth(t, c.authHeader)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status: got %d, want %d", rec.Code, http.StatusUnauthorized)
			}
			if got := rec.Header().Get("WWW-Authenticate"); got == "" {
				t.Fatal("401 response is missing the WWW-Authenticate challenge")
			}
			if nextCalled {
				t.Fatal("wrapped handler ran for a rejected request")
			}
		})
	}
}

// TestBasicAuth_Accepts confirms valid headers reach the wrapped handler,
// including a lowercase scheme: RFC 7235 §2.1 makes the auth-scheme token
// case-insensitive.
func TestBasicAuth_Accepts(t *testing.T) {
	for _, c := range []struct {
		name       string
		authHeader string
	}{
		{"CanonicalScheme", basicHeader(testLogin, testPass)},
		{"LowercaseScheme", "basic " + base64.StdEncoding.EncodeToString([]byte(testLogin+":"+testPass))},
	} {
		t.Run(c.name, func(t *testing.T) {
			rec, nextCalled := serveWithAuth(t, c.authHeader)
			if rec.Code != http.StatusOK {
				t.Fatalf("status: got %d, want %d", rec.Code, http.StatusOK)
			}
			if !nextCalled {
				t.Fatal("wrapped handler did not run for valid credentials")
			}
		})
	}
}
