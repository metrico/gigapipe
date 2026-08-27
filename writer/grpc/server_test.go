package grpc

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
