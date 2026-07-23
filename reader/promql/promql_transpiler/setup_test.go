package promql_transpiler

import (
	"os"
	"testing"

	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/qryn/v4/reader/config"
)

// TestMain initializes the package-global config.Cloki once before any test in
// this package runs.
//
// The accelerated transpiler path (TranspileExpressionV2, now the default) reads
// config.Cloki.Setting while building SQL -- StreamSelectPlanner dereferences it
// directly. Setting it here makes every test order-independent, instead of the
// package relying on whichever test happened to run first to initialize the
// global. TestTranspilerV2 runs before those initializers in source order, which
// is exactly why it panicked on a nil config.Cloki.
func TestMain(m *testing.M) {
	if config.Cloki == nil {
		config.Cloki = clconfig.New(clconfig.CLOKI_READER, nil, "", "")
	}
	os.Exit(m.Run())
}
