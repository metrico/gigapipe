package chwrapper

import (
	"context"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// We combine both ch clients: adapter.Client and main.chWrapper in one interface

type IChClient interface {
	// This should be implemented in the insert client and return error in the general purpose client
	Ping(ctx context.Context) error
	Do(ctx context.Context, query ch.Query) error

	// This should be implemented in the general purpose client and return error in the insert one
	Exec(ctx context.Context, query string, args ...any) error
	Scan(ctx context.Context, req string, args []any, dest ...interface{}) error
	DropIfEmpty(ctx context.Context, name string) error
	TableExists(ctx context.Context, name string) (bool, error)
	GetDBExec(env map[string]string) func(ctx context.Context, query string, args ...[]interface{}) error
	GetVersion(ctx context.Context, k uint64) (uint64, error)
	GetSetting(ctx context.Context, tp string, name string) (string, error)
	PutSetting(ctx context.Context, tp string, name string, value string) error
	GetFirst(req string, first ...interface{}) error
	GetList(req string) ([]string, error)
	Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error)
	QueryRow(ctx context.Context, query string, args ...interface{}) driver.Row
	// This one is shared by both
	Close() error
}

// TODO: think about the factory type
type IChClientFactory func() (IChClient, error)
