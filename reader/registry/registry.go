package registry

import (
	"crypto/tls"
	"fmt"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jmoiron/sqlx"
	"github.com/metrico/qryn/v4/reader/config"
	"github.com/metrico/qryn/v4/reader/model"
	"github.com/metrico/qryn/v4/reader/plugins"
	"github.com/metrico/qryn/v4/reader/utils/dsn"
	"github.com/metrico/qryn/v4/reader/utils/logger"
)

var Registry model.IDBRegistry

func Init() {
	p := plugins.GetDatabaseRegistryPlugin()
	if p != nil {
		Registry = (*p)()
	}
	Registry = InitStaticRegistry()
}

func Stop() {
	logger.Info("Stopping registry...")
	if Registry != nil {
		Registry.Stop()
	}
	Registry = nil
}

func InitStaticRegistry() model.IDBRegistry {
	dataDBSessions, databaseNodeMaps := createDataDBSessions()
	if len(dataDBSessions) == 0 {
		panic("We don't have any active DB session configured. Please check your config")
	}
	dbMap := map[string]*model.DataDatabasesMap{}
	for i, node := range databaseNodeMaps {
		node.Session = dataDBSessions[i]
		dbMap[node.Config.Node] = node
	}
	return NewStaticDBRegistry(dbMap)
}

// createDataDBSessions creates the DB session objects and their corresponding map structures.
func createDataDBSessions() ([]model.ISqlxDB, []*model.DataDatabasesMap) {
	dbSessions := []model.ISqlxDB{}
	dbNodeMaps := []*model.DataDatabasesMap{}
	for _, _dbObject := range config.Cloki.Setting.DATABASE_DATA {
		dbObject := _dbObject
		logger.Info(fmt.Sprintf("Connecting to [%s, %s, %s, %s, %d, %d, %d]\n", dbObject.Host, dbObject.User, dbObject.Name,
			dbObject.Node, dbObject.Port, dbObject.ReadTimeout, dbObject.WriteTimeout))

		getDB := func() *sqlx.DB {
			opts := &clickhouse.Options{
				TLS:  nil,
				Addr: []string{fmt.Sprintf("%s:%d", dbObject.Host, dbObject.Port)},
				Auth: clickhouse.Auth{
					Database: dbObject.Name,
					Username: dbObject.User,
					Password: dbObject.Password,
				},
				DialContext: nil,
				Debug:       dbObject.Debug,
				Settings:    nil,
			}
			if dbObject.Secure {
				opts.TLS = &tls.Config{InsecureSkipVerify: true}
			}
			conn := clickhouse.OpenDB(opts)
			conn.SetMaxOpenConns(dbObject.MaxOpenConn)
			conn.SetMaxIdleConns(dbObject.MaxIdleConn)
			conn.SetConnMaxLifetime(time.Minute * 10)
			db := sqlx.NewDb(conn, "clickhouse")
			db.SetMaxOpenConns(dbObject.MaxOpenConn)
			db.SetMaxIdleConns(dbObject.MaxIdleConn)
			db.SetConnMaxLifetime(time.Minute * 10)
			return db
		}
		dbSessions = append(dbSessions, &dsn.StableSqlxDBWrapper{
			DB:    getDB(),
			GetDB: getDB,
			Name:  _dbObject.Node,
		})
		chDsn := "n-clickhouse://"
		if dbObject.ClusterName != "" {
			chDsn = "c-clickhouse://"
		}
		chDsn += dbObject.User + ":" + dbObject.Password + "@" + dbObject.Host +
			strconv.FormatInt(int64(dbObject.Port), 10) + "/" + dbObject.Name
		if dbObject.Secure {
			chDsn += "?secure=true"
		}
		dbNodeMaps = append(dbNodeMaps, &model.DataDatabasesMap{
			Config: &dbObject,
			DSN:    chDsn,
		})
		logger.Info("----------------------------------- ")
		logger.Info("*** Database Config Session created *** ")
		logger.Info("----------------------------------- ")
	}
	return dbSessions, dbNodeMaps
}
