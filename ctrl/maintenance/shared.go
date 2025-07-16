package maintenance

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	clickhouse_v2 "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/ctrl/logger"
)

func ConnectV2(dbObject *config.ClokiBaseDataBase, database bool) (clickhouse_v2.Conn, error) {
	databaseName := ""
	if database {
		databaseName = dbObject.Name
	}
	opt := &clickhouse_v2.Options{
		Addr: []string{fmt.Sprintf("%s:%d", dbObject.Host, dbObject.Port)},
		Auth: clickhouse_v2.Auth{
			Database: databaseName,
			Username: dbObject.User,
			Password: dbObject.Password,
		},
		Debug:           dbObject.Debug,
		DialTimeout:     time.Second * 30,
		ReadTimeout:     time.Second * 30,
		MaxOpenConns:    10,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Hour,
		Settings: map[string]interface{}{
			"allow_experimental_database_replicated": "1",
			"materialize_ttl_after_modify":           "0",
		},
	}
	if dbObject.Secure {
		opt.TLS = &tls.Config{InsecureSkipVerify: true}
	}
	return clickhouse_v2.Open(opt)
}

func InitDBTry(conn clickhouse_v2.Conn, clusterName string, dbName string, cloud bool, logger logger.ILogger) error {
	engine := ""
	onCluster := ""
	if clusterName != "" {
		onCluster = fmt.Sprintf("ON CLUSTER `%s`", clusterName)
	}
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` %s %s", dbName, onCluster, engine)
	logger.Info("Creating database: ", query)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	err := conn.Exec(ctx, query)
	if err == nil {
		return nil
	}
	return err
}
