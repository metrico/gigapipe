package grpc

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// fakeServerStream carries a fixed context into stream interceptors under
// test; every other ServerStream method panics via the embedded nil
// interface, proving the interceptors only touch Context().
type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f fakeServerStream) Context() context.Context { return f.ctx }

// TestRecoveryInterceptor_PanicBecomesInternal asserts a panicking handler is
// recovered and surfaced as codes.Internal, mirroring the HTTP ErrorHandler.
func TestRecoveryInterceptor_PanicBecomesInternal(t *testing.T) {
	resp, err := recoveryInterceptor(
		context.Background(), nil, nil,
		func(ctx context.Context, req any) (any, error) {
			panic("boom")
		},
	)
	if err == nil {
		t.Fatal("expected non-nil error from recovered panic")
	}
	if resp != nil {
		t.Fatalf("expected nil resp, got %v", resp)
	}
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("expected codes.Internal, got %v (err=%v)", got, err)
	}
}

// TestRecoveryInterceptor_PassThrough confirms a non-panicking handler's result
// flows through unchanged.
func TestRecoveryInterceptor_PassThrough(t *testing.T) {
	want := "ok"
	resp, err := recoveryInterceptor(
		context.Background(), nil, nil,
		func(ctx context.Context, req any) (any, error) {
			return want, nil
		},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != want {
		t.Fatalf("expected %q, got %v", want, resp)
	}
}

// TestStreamRecoveryInterceptor_PanicBecomesInternal is the streaming
// counterpart of TestRecoveryInterceptor_PanicBecomesInternal.
func TestStreamRecoveryInterceptor_PanicBecomesInternal(t *testing.T) {
	err := streamRecoveryInterceptor(
		nil, fakeServerStream{ctx: context.Background()}, nil,
		func(srv any, ss grpc.ServerStream) error {
			panic("boom")
		},
	)
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("expected codes.Internal, got %v (err=%v)", got, err)
	}
}

// TestStreamRecoveryInterceptor_PassThrough confirms a non-panicking stream
// handler's error flows through unchanged.
func TestStreamRecoveryInterceptor_PassThrough(t *testing.T) {
	want := status.Error(codes.NotFound, "sentinel")
	err := streamRecoveryInterceptor(
		nil, fakeServerStream{ctx: context.Background()}, nil,
		func(srv any, ss grpc.ServerStream) error {
			return want
		},
	)
	if err != want {
		t.Fatalf("expected sentinel error, got %v", err)
	}
}

// TestStreamAuthInterceptor asserts the stream chain enforces the same
// credential checks as the unary one: a streaming RPC must never bypass
// auth. The handler-called flag proves rejection happens before any stream
// handler runs.
func TestStreamAuthInterceptor(t *testing.T) {
	interceptor := newAuthStreamInterceptor("user", "pass")
	for _, c := range []struct {
		name     string
		md       metadata.MD
		wantCode codes.Code
	}{
		{"NoMetadata", nil, codes.Unauthenticated},
		{"WrongPassword", metadata.Pairs("authorization", basicAuthHeader("user", "wrong")), codes.Unauthenticated},
		{"MalformedHeader", metadata.Pairs("authorization", "Basic not-base64!!!"), codes.Unauthenticated},
		{"CorrectCredentials", metadata.Pairs("authorization", basicAuthHeader("user", "pass")), codes.OK},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			if c.md != nil {
				ctx = metadata.NewIncomingContext(ctx, c.md)
			}
			handlerRan := false
			err := interceptor(
				nil, fakeServerStream{ctx: ctx}, nil,
				func(srv any, ss grpc.ServerStream) error {
					handlerRan = true
					return nil
				},
			)
			if got := status.Code(err); got != c.wantCode {
				t.Fatalf("expected %v, got %v (err=%v)", c.wantCode, got, err)
			}
			if wantRan := c.wantCode == codes.OK; handlerRan != wantRan {
				t.Fatalf("handlerRan=%v, want %v", handlerRan, wantRan)
			}
		})
	}
}
