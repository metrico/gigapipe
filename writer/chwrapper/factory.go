package chwrapper

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v4/writer/utils/logger"
)

func NewGeneralPurposeClient(ctx context.Context, dbObject *config.ClokiBaseDataBase, database bool) (IChClient, error) {
	logger.Info(fmt.Sprintf("Connecting to Host (SQ): [%s], User:[%s], Name:[%s], Node:[%s], Port:[%d], Timeout: [%d, %d]\n",
		dbObject.Host, dbObject.User, dbObject.Name, dbObject.Node,
		dbObject.Port, dbObject.ReadTimeout, dbObject.WriteTimeout))
	databaseName := ""
	if database {
		databaseName = dbObject.Name
	}

	opt := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", dbObject.Host, dbObject.Port)},
		Auth: clickhouse.Auth{
			Database: databaseName,
			Username: dbObject.User,
			Password: dbObject.Password,
		},
		MaxIdleConns: dbObject.MaxIdleConn,
		MaxOpenConns: dbObject.MaxOpenConn,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 5 * time.Second,
	}
	if dbObject.Secure {
		opt.TLS = &tls.Config{
			InsecureSkipVerify: dbObject.InsecureSkipVerify,
		}
	}
	conn, err := clickhouse.Open(opt)
	if err != nil {
		return nil, err
	}

	err = conn.Ping(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not initialize clickhouse connection: %v", err)
	}

	return &Client{
		c: conn,
	}, nil
}

func NewGeneralPurposeClientWithXDSN(ctx context.Context, Xdsn string, database bool) (IChClient, error) {
	// Extract the prefix (n- or c-) from the X-DSN
	if len(Xdsn) < 2 {
		return nil, fmt.Errorf("invalid X-DSN format: %s", Xdsn)
	}
	dsn := Xdsn[2:] // The rest is the actual ClickHouse DSN

	return NewGeneralPurposeClientWithDSN(ctx, dsn, database)
}

// NewGeneralPurposeClientWithDSN initializes a ClickHouse client using a  string.
func NewGeneralPurposeClientWithDSN(ctx context.Context, dsn string, database bool) (IChClient, error) {

	dsnOpts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	dsnOpts.Compression = &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	}

	dsnOpts.Settings = clickhouse.Settings{
		"max_execution_time": 60,
	}

	// Establish the connection
	conn, err := clickhouse.Open(dsnOpts)
	if err != nil {
		return nil, fmt.Errorf("could not connect to ClickHouse with DSN %s: %v", dsn, err)
	}

	// Ping to verify the connection
	err = conn.Ping(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not ping ClickHouse: %v", err)
	}

	// Return the ClickHouse client implementing the IChClient interface
	return &Client{
		c: conn,
	}, nil

}

func NewWriterClient(ctx context.Context, dbObject *config.ClokiBaseDataBase, database bool) (IChClient, error) {
	to, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	db := ""
	if database {
		db = dbObject.Name
	}
	opts := ch.Options{
		Address:     fmt.Sprintf("%s:%d", dbObject.Host, dbObject.Port),
		Database:    db,
		User:        dbObject.User,
		Password:    dbObject.Password,
		DialTimeout: time.Second * 30,
	}
	if dbObject.Secure {
		opts.Dialer = &v3SecureDialer{dbObject.InsecureSkipVerify}
	}
	DSN := "n-"
	if dbObject.ClusterName != "" {
		DSN = "c-"
	}
	DSN += fmt.Sprintf("clickhouse://%s:%d/%s?secure=", dbObject.Host, dbObject.Port, db)
	if dbObject.Secure {
		DSN += "true"
	} else {
		DSN += "false"
	}
	client, err := ch.Dial(to, opts)
	if err != nil {
		return nil, fmt.Errorf("%s: %v", DSN, err)
	}
	err = client.Ping(context.Background())
	if err != nil {
		client.Close()
		return nil, err
	}

	return &InsertCHWrapper{
		Client: client,
	}, nil
}

// NewWriterClientWithXDSN initializes a ClickHouse client using an X-DSN string.
func NewWriterClientWithXDSN(ctx context.Context, Xdsn string, database bool) (IChClient, error) {

	// Extract the prefix (n- or c-) from the X-DSN
	if len(Xdsn) < 2 {
		return nil, fmt.Errorf("invalid X-DSN format: %s", Xdsn)
	}
	dsn := Xdsn[2:] // The rest is the actual ClickHouse DSN

	return NewWriterClientWithDSN(ctx, dsn, database)

}

// NewWriterClientWithDSN initializes a ClickHouse client using a  string.
func NewWriterClientWithDSN(ctx context.Context, dsn string, database bool) (IChClient, error) {
	to, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Parse the DSN string
	dsnOpts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DSN: %v", err)
	}

	addresses := strings.Join(dsnOpts.Addr, ",")

	// Prepare ClickHouse connection options
	opts := ch.Options{
		Address:  addresses, // This is a slice of addresses, supporting multiple hosts for load-balancing and failover
		Database: dsnOpts.Auth.Database,
		User:     dsnOpts.Auth.Username,
		Password: dsnOpts.Auth.Password,
	}

	// Add TLS configuration if present in DSN
	if dsnOpts.TLS != nil {
		opts.Dialer = &tls.Dialer{
			Config: &tls.Config{
				InsecureSkipVerify: dsnOpts.TLS.InsecureSkipVerify,
			},
		}
	}

	// Establish connection using the ch-go library
	client, err := ch.Dial(to, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %v", err)
	}

	// Ping to verify the connection
	err = client.Ping(to)
	if err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %v", err)
	}

	// Return the client wrapper with the original DSN
	return &InsertCHWrapper{
		Client: client,
	}, nil
}

// NewSmartDatabaseAdapter initializes a SmartDatabaseAdapter using the given ClokiBaseDataBase config object.
func NewSmartDatabaseAdapter(dbObject *config.ClokiBaseDataBase, database bool) (IChClient, error) {

	if dbObject == nil {
		return nil, fmt.Errorf("dbObject cannot be nil")
	}
	return &SmartDatabaseAdapter{
		dbObject: dbObject,
		database: database,
	}, nil
}

// NewSmartDatabaseAdapterWithXDSN initializes a SmartDatabaseAdapter using an X-DSN string.
func NewSmartDatabaseAdapterWithXDSN(Xdsn string, database bool) (IChClient, error) {
	if Xdsn == "" {
		return nil, fmt.Errorf("X-DSN cannot be empty")
	}

	return &SmartDatabaseAdapter{
		Xdsn:     Xdsn,
		database: database,
	}, nil
}

// NewSmartDatabaseAdapterWithDSN initializes a SmartDatabaseAdapter using a ClickHouse DSN string.
func NewSmartDatabaseAdapterWithDSN(dsn string, database bool) (IChClient, error) {

	if dsn == "" {
		return nil, fmt.Errorf("DSN cannot be empty")
	}
	return &SmartDatabaseAdapter{
		DSN:      dsn,
		database: database,
	}, nil
}
