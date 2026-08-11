package grpc

import (
	"context"
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
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		auth := mdFirst(md, "authorization")
		if auth == "" {
			return nil, status.Error(codes.Unauthenticated, invalidCredentialsMsg)
		}

		parts := strings.SplitN(auth, " ", 2)
		if len(parts) != 2 || parts[0] != "Basic" {
			return nil, status.Error(codes.Unauthenticated, invalidCredentialsMsg)
		}

		payload, err := base64.StdEncoding.DecodeString(parts[1])
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, invalidCredentialsMsg)
		}

		pair := strings.SplitN(string(payload), ":", 2)
		if len(pair) != 2 {
			return nil, status.Error(codes.Unauthenticated, invalidCredentialsMsg)
		}

		// crypto/subtle.ConstantTimeCompare, not ==: unlike the HTTP
		// reference, comparisons here must not leak timing information about
		// how many leading bytes matched.
		userOK := subtle.ConstantTimeCompare([]byte(pair[0]), []byte(user)) == 1
		passOK := subtle.ConstantTimeCompare([]byte(pair[1]), []byte(pass)) == 1
		if !userOK || !passOK {
			return nil, status.Error(codes.Unauthenticated, invalidCredentialsMsg)
		}

		return handler(ctx, req)
	}
}
