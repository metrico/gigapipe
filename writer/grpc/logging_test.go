package grpc

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
)

// TestFormatAccessLog table-tests formatAccessLog over a range of
// (method, code, duration) combinations, including a non-OK code, without
// capturing logger output.
func TestFormatAccessLog(t *testing.T) {
	cases := []struct {
		name       string
		fullMethod string
		code       codes.Code
		d          time.Duration
		wantParts  []string
	}{
		{
			name:       "OK trace export",
			fullMethod: "/opentelemetry.proto.collector.trace.v1.TraceService/Export",
			code:       codes.OK,
			d:          1200 * time.Microsecond,
			wantParts: []string{
				"[OK]",
				"gRPC /opentelemetry.proto.collector.trace.v1.TraceService/Export",
				"LAT:",
			},
		},
		{
			name:       "Unauthenticated",
			fullMethod: "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
			code:       codes.Unauthenticated,
			d:          500 * time.Microsecond,
			wantParts: []string{
				"[Unauthenticated]",
				"gRPC /opentelemetry.proto.collector.logs.v1.LogsService/Export",
				"LAT:",
			},
		},
		{
			name:       "Internal (panic recovered)",
			fullMethod: "/opentelemetry.proto.collector.profiles.v1development.ProfilesService/Export",
			code:       codes.Internal,
			d:          10 * time.Millisecond,
			wantParts: []string{
				"[Internal]",
				"gRPC /opentelemetry.proto.collector.profiles.v1development.ProfilesService/Export",
				"LAT:",
			},
		},
		{
			name:       "zero duration",
			fullMethod: "/x/Y",
			code:       codes.OK,
			d:          0,
			wantParts: []string{
				"[OK]",
				"gRPC /x/Y",
				"LAT:0s",
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := formatAccessLog(c.fullMethod, c.code, c.d)
			for _, want := range c.wantParts {
				if !strings.Contains(got, want) {
					t.Fatalf("formatAccessLog(%q, %v, %v) = %q; want substring %q", c.fullMethod, c.code, c.d, got, want)
				}
			}
		})
	}
}
