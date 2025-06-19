package ch_wrapper

import (
	"context"
	"fmt"
	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestNewHttpChClientFactory(t *testing.T) {
	//should be DSN Value.
	dsn := "http://default:PW@localhost:8234/default"

	factory := NewHttpChClientFactory(dsn)
	require.NotNil(t, factory, "Factory should not be nil")

	client, err := factory()
	if err != nil {
		t.Logf("Factory error: %v", err)
	}
	require.NoError(t, err, "Expected no error when creating HttpChClient")
	require.NotNil(t, client, "Expected non-nil HttpChClient")

	// Add timeout context for all operations
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test connection with timeout
	err = client.Ping(ctx)
	if err != nil {
		t.Logf("Ping failed: %v", err)
		t.Skip("ClickHouse server not available, skipping test")
	}
	require.NoError(t, err, "Expected Ping to succeed")

	// Test simple query
	var res uint8
	err = client.GetFirst("SELECT 1", &res)
	if err != nil {
		t.Logf("GetFirst error: %v", err)
	}
	assert.NoError(t, err, "SELECT 1 should work")
	assert.Equal(t, uint8(1), res, "Expected result to be 1")
	fmt.Printf("SELECT 1 result: %d\n", res)

	// Create table with better error handling
	tableName := fmt.Sprintf("exp_test_%d", time.Now().Unix())
	createTableQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (a UInt8) ENGINE=MergeTree ORDER BY ()", tableName)

	err = client.Exec(ctx, createTableQuery)
	if err != nil {
		t.Logf("Create table error: %v", err)
	}
	assert.NoError(t, err, "Table creation should succeed")

	// Insert data with proper error handling
	insertQuery := fmt.Sprintf("INSERT INTO %s (a)", tableName)
	err = client.Do(ctx, ch.Query{
		Body: insertQuery,
		Input: proto.Input{
			proto.InputColumn{
				Name: "a",
				Data: proto.ColUInt8{1},
			},
		},
	})
	if err != nil {
		t.Logf("Insert error: %v", err)
	}
	assert.NoError(t, err, "Insert should succeed")

	// Add a small delay to ensure data is written
	time.Sleep(100 * time.Millisecond)

	// Test count query using GetFirst method
	var count uint64
	countQuery := fmt.Sprintf("SELECT count() FROM %s", tableName)
	err = client.GetFirst(countQuery, &count)
	if err != nil {
		t.Logf("Count query error: %v", err)
	}
	assert.NoError(t, err, "Count query should succeed")
	assert.Equal(t, uint64(1), count, "Expected count to be 1")
	fmt.Printf("Count result: %d\n", count)

	// Cleanup
	dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	err = client.Exec(ctx, dropQuery)
	if err != nil {
		t.Logf("Cleanup error: %v", err)
	}
}
