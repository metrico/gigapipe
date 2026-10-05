package ctrl

import (
	"testing"

	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/cloki-config/config"
)

func TestImportMetricsDispatchesThroughTheProjectTable(t *testing.T) {
	cfg := &clconfig.ClokiConfig{Setting: &config.ClokiBaseSettingServer{}}
	if err := ImportMetrics(cfg, "unknown"); err == nil {
		t.Error("ImportMetrics ran for an unknown project")
	}
	if err := ImportMetrics(cfg, "gigapipe"); err != nil {
		t.Errorf("ImportMetrics(gigapipe) = %v", err)
	}
}
