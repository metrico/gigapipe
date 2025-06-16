package ch_wrapper

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewHttpChClientFactory(t *testing.T) {
	//should be DSN Value.
	dsn := ""

	factory := NewHttpChClientFactory(dsn)

	client, err := factory()
	require.NoError(t, err, "Expected no error when creating HttpChClient")
	require.NotNil(t, client, "Expected non-nil HttpChClient")

	// No need for type assertion — just call Ping
	err = client.Ping(context.Background())
	require.NoError(t, err, "Expected Ping to succeed")
}
