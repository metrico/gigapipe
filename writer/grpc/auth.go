package grpc

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// invalidCredentialsMsg is the single, generic message returned for every
// auth failure (missing header, malformed header, bad base64, wrong
// credentials). Deliberately generic: it must not tell a caller which part
// failed. There is no gRPC analogue of HTTP's WWW-Authenticate response
// header, so none is emulated here.
const invalidCredentialsMsg = "invalid credentials"

// newAuthInterceptor returns a unary interceptor that requires HTTP Basic
// credentials in the "authorization" gRPC metadata key, mirroring
// reader/utils/middleware/basic_auth.go's BasicAuthMiddleware but for gRPC.
//
// An OpenTelemetry Collector authenticates against this with its `basicauth`
// extension, which sets exactly this metadata key/value shape
// ("authorization: Basic <base64(user:pass)>") on every RPC.
//
// This is checked on a cleartext port: like the HTTP path, credentials
// travel unencrypted unless TLS terminates in front of it, so this should
// only be exposed off a trusted network, or behind TLS.
//
// Callers gate construction on user/pass both being non-empty (see
// NewServer) — this function itself does not special-case empty
// credentials, so the auth failure path also protects against a
// misconfigured empty user or pass.
func newAuthInterceptor(user, pass string) grpc.UnaryServerInterceptor {
	check := newCredentialCheck(user, pass)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := check(ctx); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// newAuthStreamInterceptor is newAuthInterceptor's counterpart for streaming
// RPCs: it verifies the same "authorization" metadata once, when the stream
// is opened, before any handler runs. No registered service declares
// streaming methods today; this exists so that the first one added is
// authenticated instead of silently bypassing auth.
func newAuthStreamInterceptor(user, pass string) grpc.StreamServerInterceptor {
	check := newCredentialCheck(user, pass)
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := check(ss.Context()); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

// newCredentialCheck returns the credential verification shared by the unary
// and stream auth interceptors: it inspects the context's "authorization"
// metadata and returns nil on valid Basic credentials, or a
// codes.Unauthenticated status otherwise.
func newCredentialCheck(user, pass string) func(ctx context.Context) error {
	// Hash the configured credentials once, not per request. Comparing
	// SHA-256 digests rather than the raw strings keeps both operands a
	// fixed 32 bytes, so the comparison below cannot leak credential length
	// (subtle.ConstantTimeCompare returns early when lengths differ).
	expectedUser := sha256.Sum256([]byte(user))
	expectedPass := sha256.Sum256([]byte(pass))

	return func(ctx context.Context) error {
		md, _ := metadata.FromIncomingContext(ctx)
		auth := mdFirst(md, "authorization")
		if auth == "" {
			return status.Error(codes.Unauthenticated, invalidCredentialsMsg)
		}

		// The auth-scheme token is case-insensitive (RFC 7235 §2.1).
		parts := strings.SplitN(auth, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Basic") {
			return status.Error(codes.Unauthenticated, invalidCredentialsMsg)
		}

		payload, err := base64.StdEncoding.DecodeString(parts[1])
		if err != nil {
			return status.Error(codes.Unauthenticated, invalidCredentialsMsg)
		}

		pair := strings.SplitN(string(payload), ":", 2)
		if len(pair) != 2 {
			return status.Error(codes.Unauthenticated, invalidCredentialsMsg)
		}

		// Both comparisons are evaluated before branching: folding them
		// into one short-circuiting condition would leak, by timing,
		// whether the user alone was correct.
		gotUser := sha256.Sum256([]byte(pair[0]))
		gotPass := sha256.Sum256([]byte(pair[1]))
		userOK := subtle.ConstantTimeCompare(gotUser[:], expectedUser[:]) == 1
		passOK := subtle.ConstantTimeCompare(gotPass[:], expectedPass[:]) == 1
		if !userOK || !passOK {
			return status.Error(codes.Unauthenticated, invalidCredentialsMsg)
		}

		return nil
	}
}
