package chwrapper

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/ClickHouse/ch-go"
	config2 "github.com/metrico/cloki-config/config"
	"github.com/stretchr/testify/assert"
)

func TestNewInsertCHClient(t *testing.T) {
	if os.Getenv("CLICKHOUSE_HOST") == "" {
		return
	}
	port, err := strconv.Atoi(os.Getenv("CLICKHOUSE_PORT"))
	assert.NoError(t, err)
	ctx := context.Background()
	cfg := config2.ClokiBaseDataBase{
		User:     os.Getenv("CLICKHOUSE_USER"),
		Node:     "ch",
		Password: os.Getenv("CLICKHOUSE_PASSWORD"),
		Name:     os.Getenv("CLICKHOUSE_DATABASE"),
		Host:     os.Getenv("CLICKHOUSE_HOST"),
		Port:     uint32(port),
	}

	smartAdapter, err := NewSmartDatabaseAdapter(&cfg, true)
	assert.NoError(t, err)
	assert.NotNil(t, smartAdapter)
	var res uint8
	err = smartAdapter.GetFirst("SELECT 1", &res)
	assert.NoError(t, err)
	fmt.Println(res)

	createTableQuery := "CREATE TABLE IF NOT EXISTS exp (a UInt8) ENGINE=MergeTree ORDER BY ()"
	err = smartAdapter.Exec(ctx, createTableQuery)
	assert.NoError(t, err)

	err = smartAdapter.Do(ctx, ch.Query{
		Body: "INSERT INTO exp (a) VALUES (1)",
	})

	assert.NoError(t, err)

	// 4. Test Select count() from the table
	var count uint64
	err = smartAdapter.Scan(ctx, "SELECT count() FROM exp", nil, &count)
	assert.NoError(t, err)
	fmt.Println(count)
}

func TestNewInsertCHClientXDSN(t *testing.T) {
	ctx := context.Background()
	Xdsn := os.Getenv("CLICKHOUSE_XDSN")
	if Xdsn == "" {
		fmt.Println("CLICKHOUSE_DSN environment variable is not set")
		return
	}
	smartAdapter, err := NewSmartDatabaseAdapterWithXDSN(Xdsn, true)
	assert.NoError(t, err)
	assert.NotNil(t, smartAdapter)
	// 1. Test Select 1 using the General Purpose Client
	err = smartAdapter.Exec(ctx, "SELECT 1")
	assert.NoError(t, err)

	// 2. Test Create Table
	createTableQuery := "CREATE TABLE IF NOT EXISTS exp (a UInt8) ENGINE=MergeTree ORDER BY ()"
	err = smartAdapter.Exec(ctx, createTableQuery)
	assert.NoError(t, err)

	// 3. Test Insert a row into the table
	err = smartAdapter.Do(ctx, ch.Query{
		Body: "INSERT INTO exp (a) VALUES (1)",
	})
	assert.NoError(t, err)

	// 4. Test Select count() from the table
	var count uint64
	err = smartAdapter.Scan(ctx, "SELECT count() FROM exp", nil, &count)
	assert.NoError(t, err)
	fmt.Println(count)
}

func TestNewInsertCHClientWithOutDSN(t *testing.T) {
	ctx := context.Background()
	dsn := os.Getenv("CLICKHOUSE_DSN")
	if dsn == "" {
		fmt.Println("CLICKHOUSE_DSN environment variable is not set")
		return
	}
	smartAdapter, err := NewSmartDatabaseAdapterWithDSN(dsn, true)
	assert.NoError(t, err)
	assert.NotNil(t, smartAdapter)

	// 1. Test Select 1 using the General Purpose Client
	err = smartAdapter.Exec(ctx, "SELECT 1")
	assert.NoError(t, err)

	// 2. Test Create Table
	createTableQuery := "CREATE TABLE IF NOT EXISTS exp (a UInt8) ENGINE=MergeTree ORDER BY ()"
	err = smartAdapter.Exec(ctx, createTableQuery)
	assert.NoError(t, err)

	// 3. Test Insert a row into the table
	err = smartAdapter.Do(ctx, ch.Query{
		Body: "INSERT INTO exp (a) VALUES (1)",
	})
	assert.NoError(t, err)

	// 4. Test Select count() from the table
	var count uint64
	err = smartAdapter.Scan(ctx, "SELECT count() FROM exp", nil, &count)
	assert.NoError(t, err)
	fmt.Println(count)
}
