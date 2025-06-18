package ch_wrapper

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewHttpChClientFactory(t *testing.T) {
	//should be DSN Value.
	dsn := "http://default:PW@localhost:8234/default"

	factory := NewHttpChClientFactory(dsn)

	client, err := factory()
	if err != nil {
		fmt.Println(err.Error())
	}
	require.NoError(t, err, "Expected no error when creating HttpChClient")
	require.NotNil(t, client, "Expected non-nil HttpChClient")

	// No need for type assertion — just call Ping
	err = client.Ping(context.Background())
	require.NoError(t, err, "Expected Ping to succeed")
}
