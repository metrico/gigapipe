package middleware

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"net/http"
	"strings"
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
			auth := r.Header.Get("Authorization")
			if auth == "" {
				w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}

			authParts := strings.SplitN(auth, " ", 2)
			if len(authParts) != 2 || authParts[0] != "Basic" {
				http.Error(w, "Invalid authorization header", http.StatusBadRequest)
				return
			}

			// A decode error must be handled: DecodeString returns the bytes
			// decoded before the failure, so ignoring it would let malformed
			// input fall through as partially-decoded garbage and be reported
			// as wrong credentials rather than as a malformed header.
			payload, err := base64.StdEncoding.DecodeString(authParts[1])
			if err != nil {
				http.Error(w, "Invalid authorization header", http.StatusBadRequest)
				return
			}

			pair := strings.SplitN(string(payload), ":", 2)
			if len(pair) != 2 {
				http.Error(w, "Invalid authorization header", http.StatusBadRequest)
				return
			}

			// Both comparisons are evaluated before branching: folding them
			// into one short-circuiting condition would leak, by timing,
			// whether the login alone was correct.
			gotLogin := sha256.Sum256([]byte(pair[0]))
			gotPass := sha256.Sum256([]byte(pair[1]))
			loginOK := subtle.ConstantTimeCompare(gotLogin[:], expectedLogin[:]) == 1
			passOK := subtle.ConstantTimeCompare(gotPass[:], expectedPass[:]) == 1
			if !loginOK || !passOK {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
