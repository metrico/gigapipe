package grpc

import (
	"context"
	"testing"

	"github.com/metrico/qryn/v5/writer/service"
	"github.com/metrico/qryn/v5/writer/utils"
	"google.golang.org/grpc/metadata"
)

func TestDSNCtx_ReadsMetadata(t *testing.T) {
	md := metadata.New(map[string]string{"x-ch-dsn": "clickhouse://x"})
	ctx := metadata.NewIncomingContext(context.Background(), md)
	dsn, _ := dsnCtx(ctx)
	if dsn != "clickhouse://x" {
		t.Fatalf("got %q", dsn)
	}
}

func TestDSNCtx_EmptyWhenAbsent(t *testing.T) {
	dsn, _ := dsnCtx(context.Background())
	if dsn != "" {
		t.Fatalf("expected empty dsn, got %q", dsn)
	}
}

func TestDSNCtx_ParsesTTLDays(t *testing.T) {
	// Valid value parses to uint16.
	md := metadata.New(map[string]string{"x-ttl-days": "7"})
	ctx := metadata.NewIncomingContext(context.Background(), md)
	_, enriched := dsnCtx(ctx)
	if got := enriched.Value(utils.ContextKeyTTLDays); got != uint16(7) {
		t.Fatalf("x-ttl-days=7: got %#v, want uint16(7)", got)
	}

	// Absent metadata defaults to uint16(0).
	_, enriched = dsnCtx(context.Background())
	if got := enriched.Value(utils.ContextKeyTTLDays); got != uint16(0) {
		t.Fatalf("absent x-ttl-days: got %#v, want uint16(0)", got)
	}

	// Invalid value defaults to uint16(0).
	md = metadata.New(map[string]string{"x-ttl-days": "notanumber"})
	ctx = metadata.NewIncomingContext(context.Background(), md)
	_, enriched = dsnCtx(ctx)
	if got := enriched.Value(utils.ContextKeyTTLDays); got != uint16(0) {
		t.Fatalf("invalid x-ttl-days: got %#v, want uint16(0)", got)
	}
}

func TestDSNCtx_ParsesAsyncMode(t *testing.T) {
	cases := []struct {
		val  string // "" means metadata absent
		want int
	}{
		{"1", service.INSERT_MODE_ASYNC},
		{"0", service.INSERT_MODE_SYNC},
		{"", service.INSERT_MODE_DEFAULT},
	}
	for _, c := range cases {
		ctx := context.Background()
		if c.val != "" {
			md := metadata.New(map[string]string{"x-async-insert": c.val})
			ctx = metadata.NewIncomingContext(ctx, md)
		}
		_, enriched := dsnCtx(ctx)
		got := enriched.Value(utils.ContextKeyAsync)
		if gi, ok := got.(int); !ok || gi != c.want {
			t.Fatalf("x-async-insert=%q: got %#v, want int(%d)", c.val, got, c.want)
		}
	}
}
