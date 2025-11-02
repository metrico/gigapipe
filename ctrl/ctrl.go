package ctrl

import (
	"fmt"

	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v4/ctrl/qryn/maintenance"
	"github.com/metrico/qryn/v4/logger"
	"github.com/sirupsen/logrus"
)

var projects = map[string]struct {
	init    func(config *config.ClokiBaseDataBase, logger *logrus.Logger) error
	upgrade func(config []config.ClokiBaseDataBase, logger *logrus.Logger) error
	rotate  func(base []config.ClokiBaseDataBase, logger *logrus.Logger) error
}{
	"qryn": {
		maintenance.InitDB,
		maintenance.UpgradeAll,
		maintenance.RotateAll,
	},
}

func Init(config *clconfig.ClokiConfig, project string) error {
	var err error
	logger.Info("Initializing ctrl module...")
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
