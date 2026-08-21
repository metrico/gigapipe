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
// status code AND that the wrapped handler never ran -- a status assertion
// alone would not prove the request was actually blocked.
func TestBasicAuth_Rejects(t *testing.T) {
	for _, c := range []struct {
		name       string
		authHeader string
		wantStatus int
	}{
		{"MissingHeader", "", http.StatusUnauthorized},
		{"NoScheme", "abc", http.StatusBadRequest},
		{"BearerScheme", "Bearer " + base64.StdEncoding.EncodeToString([]byte("user:s3cret")), http.StatusBadRequest},
		// Guards the previously-swallowed base64 decode error: "!!!!" decodes
		// to partial garbage, which must be reported as a malformed header
		// rather than falling through to the credential comparison.
		{"NonBase64Payload", "Basic !!!!", http.StatusBadRequest},
		{"NoColonInPayload", "Basic " + base64.StdEncoding.EncodeToString([]byte("usernopass")), http.StatusBadRequest},
		{"WrongLogin", basicHeader("wrong", testPass), http.StatusUnauthorized},
		{"WrongPass", basicHeader(testLogin, "wrong"), http.StatusUnauthorized},
		{"EmptyCredentials", basicHeader("", ""), http.StatusUnauthorized},
		// A prefix of the real password must not be accepted.
		{"PassPrefix", basicHeader(testLogin, "s3cre"), http.StatusUnauthorized},
	} {
		t.Run(c.name, func(t *testing.T) {
			rec, nextCalled := serveWithAuth(t, c.authHeader)
			if rec.Code != c.wantStatus {
				t.Fatalf("status: got %d, want %d", rec.Code, c.wantStatus)
			}
			if nextCalled {
				t.Fatal("wrapped handler ran for a rejected request")
			}
		})
	}
}

// TestBasicAuth_AcceptsCorrectCredentials confirms a valid header reaches the
// wrapped handler.
func TestBasicAuth_AcceptsCorrectCredentials(t *testing.T) {
	rec, nextCalled := serveWithAuth(t, basicHeader(testLogin, testPass))
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusOK)
	}
	if !nextCalled {
		t.Fatal("wrapped handler did not run for valid credentials")
	}
}

// TestBasicAuth_ChallengeOnMissingHeader pins the existing behavior: the
// challenge is sent only when no Authorization header was supplied, not on a
// wrong-credentials rejection.
func TestBasicAuth_ChallengeOnMissingHeader(t *testing.T) {
	rec, _ := serveWithAuth(t, "")
	if got := rec.Header().Get("WWW-Authenticate"); got == "" {
		t.Fatal("expected a WWW-Authenticate challenge when no header was sent")
	}

	rec, _ = serveWithAuth(t, basicHeader(testLogin, "wrong"))
	if got := rec.Header().Get("WWW-Authenticate"); got != "" {
		t.Fatalf("expected no challenge on wrong credentials, got %q", got)
	}
}
