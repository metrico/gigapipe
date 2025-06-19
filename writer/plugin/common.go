package plugin

import (
	"github.com/metrico/cloki-config/config"
)

type ConfigInitializer interface {
	InitializeConfig(conf *config.ClokiBaseSettingServer) error
}
