package ctrl

import (
	"context"
	"fmt"
	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/ctrl/logger"
	"github.com/metrico/qryn/v5/ctrl/qryn/maintenance"
)

var projects = map[string]struct {
	init    func(*config.ClokiBaseDataBase, logger.ILogger) error
	upgrade func(config []config.ClokiBaseDataBase, logger logger.ILogger) error
	rotate  func(base []config.ClokiBaseDataBase, logger logger.ILogger) error
}{
	"gigapipe": {
		maintenance.InitDB,
		maintenance.UpgradeAll,
		maintenance.RotateAll,
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

// PrepareMetricMigration checks the metric migration of each database and returns its run.
func PrepareMetricMigration(config *clconfig.ClokiConfig) (func(context.Context) ([]maintenance.MetricMigrationReport, error), error) {
	m, err := maintenance.PrepareMetricMigration(config.Setting.DATABASE_DATA)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context) ([]maintenance.MetricMigrationReport, error) {
		return m.Run(ctx, logger.Logger)
	}, nil
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
