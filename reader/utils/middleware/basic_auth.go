package middleware

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
)

func BasicAuthMiddleware(login, pass string) func(next http.Handler) http.Handler {
	// Hash the configured credentials once, not per request. Comparing
	// SHA-256 digests rather than the raw strings keeps both operands a
	// fixed 32 bytes, so the comparison below cannot leak credential length
	// (subtle.ConstantTimeCompare returns early when lengths differ).
	expectedLogin := sha256.Sum256([]byte(login))
	expectedPass := sha256.Sum256([]byte(pass))

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// r.BasicAuth matches the "Basic" scheme case-insensitively, as
			// RFC 7235 §2.1 requires, and reports a missing or malformed
			// header as !ok.
			username, password, ok := r.BasicAuth()

			// Both comparisons are evaluated before branching: folding them
			// into one short-circuiting condition would leak, by timing,
			// whether the login alone was correct.
			gotLogin := sha256.Sum256([]byte(username))
			gotPass := sha256.Sum256([]byte(password))
			loginOK := subtle.ConstantTimeCompare(gotLogin[:], expectedLogin[:]) == 1
			passOK := subtle.ConstantTimeCompare(gotPass[:], expectedPass[:]) == 1
			if !ok || !loginOK || !passOK {
				// RFC 7235 §4.1: every 401 response MUST carry a
				// WWW-Authenticate challenge; user agents rely on it to
				// (re)prompt for credentials.
				w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
