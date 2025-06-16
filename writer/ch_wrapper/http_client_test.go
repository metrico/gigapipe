package ch_wrapper

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewHttpChClientFactory(t *testing.T) {
	// Replace this with your actual ClickHouse HTTP endpoint
	dsn := "clickhouse1://default:Wx0atcObIPZLeQ@localhost:9556/default"

	factory := NewHttpChClientFactory(dsn)

	client, err := factory()
	require.NoError(t, err, "Expected no error when creating HttpChClient")
	require.NotNil(t, client, "Expected non-nil HttpChClient")
	err = client.Ping(context.Background())
	if err == nil {
		fmt.Println("ping success")
	}
	require.NoError(t, err, "Expected Ping to succeed")

}
