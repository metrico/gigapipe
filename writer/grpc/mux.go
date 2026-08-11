package grpc

import (
	"net/http"
	"strings"
)

// Mux returns an http.Handler that routes gRPC requests to a gRPC server
// built by NewServer, and every other request to next. Dispatch is by
// transport shape, not by path: HTTP/2 requests whose Content-Type starts
// with "application/grpc" (matching "application/grpc", "application/grpc+proto"
// and "application/grpc-web") go to the gRPC server; everything else goes to
// next unchanged.
//
// Two limitations are accepted here, not fixed:
//
//  1. gRPC requests bypass the mux middleware chain entirely (logging, CORS,
//     AcceptEncodingMiddleware and, notably, BasicAuthMiddleware). If
//     AUTH_SETTINGS.BASIC is configured, OTLP/HTTP is authenticated but
//     OTLP/gRPC is not. This is a known gap; adding a gRPC auth interceptor
//     is deliberately left as follow-up work, not addressed here.
//  2. grpc-go's Server.GracefulStop does not drain RPCs served through
//     ServeHTTP (as used here) — it only affects connections accepted via
//     Server.Serve. When gRPC is multiplexed onto the shared http.Server like
//     this, draining in-flight gRPC requests is the responsibility of
//     http.Server.Shutdown alone.
func Mux(next http.Handler) http.Handler {
	g := NewServer()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			g.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Protocols returns the http.Server protocol set required to accept gRPC on
// a cleartext port: HTTP/1 plus unencrypted (prior-knowledge) HTTP/2. This is
// what lets a single http.Server serve both regular HTTP/1.1 traffic and
// gRPC's HTTP/2 traffic on one address without TLS.
func Protocols() *http.Protocols {
	p := &http.Protocols{}
	p.SetHTTP1(true)
	p.SetUnencryptedHTTP2(true)
	return p
}
