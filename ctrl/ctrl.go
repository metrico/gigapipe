package ctrl

import (
	"fmt"
	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/ctrl/logger"
	"github.com/metrico/qryn/v5/ctrl/qryn/maintenance"
)

var projects = map[string]struct {
	init          func(*config.ClokiBaseDataBase, logger.ILogger) error
	upgrade       func(config []config.ClokiBaseDataBase, logger logger.ILogger) error
	rotate        func(base []config.ClokiBaseDataBase, logger logger.ILogger) error
	importMetrics func(base []config.ClokiBaseDataBase, logger logger.ILogger)
}{
	"gigapipe": {
		maintenance.InitDB,
		maintenance.UpgradeAll,
		maintenance.RotateAll,
		maintenance.ImportAllMetrics,
	},
}

func Init(config *clconfig.ClokiConfig, project string) error {
	var err error
	proj, ok := projects[project]
	if !ok {
		return fmt.Errorf("project %s not found", project)
	}

	for _, db := range config.Setting.DATABASE_DATA {
		err = proj.init(&db, logger.Logger)
		if err != nil {
			panic(err)
		}
	}
	err = proj.upgrade(config.Setting.DATABASE_DATA, logger.Logger)
	return err
}

// ImportMetrics starts the metric import of each database in the background.
func ImportMetrics(config *clconfig.ClokiConfig, project string) error {
	proj, ok := projects[project]
	if !ok {
		return fmt.Errorf("project %s not found", project)
	}
	proj.importMetrics(config.Setting.DATABASE_DATA, logger.Logger)
	return nil
}

func Rotate(config *clconfig.ClokiConfig, project string) error {
	var err error
	proj, ok := projects[project]
	if !ok {
		return fmt.Errorf("project %s not found", project)
	}

	for _, db := range config.Setting.DATABASE_DATA {
		err = proj.init(&db, logger.Logger)
		if err != nil {
			panic(err)
		}
	}
	err = proj.rotate(config.Setting.DATABASE_DATA, logger.Logger)
	return err
}
