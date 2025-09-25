package chwrapper

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type InsertCHWrapper struct {
	mutex sync.Mutex
	*ch.Client
}

var _ IChClient = &InsertCHWrapper{}

func (c *InsertCHWrapper) Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	//TODO implement me
	panic("implement me")
}

func (c *InsertCHWrapper) QueryRow(ctx context.Context, query string, args ...interface{}) driver.Row {
	//TODO implement me
	panic("implement me")
}

func (c *InsertCHWrapper) Do(ctx context.Context, query ch.Query) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.Client.Do(ctx, query)
}

func (c *InsertCHWrapper) Ping(ctx context.Context) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.Client.Ping(ctx)
}

func (c *InsertCHWrapper) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.Client.Close()
}

type v3SecureDialer struct {
	InsecureSkipVerify bool
}

func (v *v3SecureDialer) DialContext(_ context.Context, network string, address string) (net.Conn, error) {
	return tls.Dial(network, address, &tls.Config{InsecureSkipVerify: v.InsecureSkipVerify})
}

// Methods that should return errors in InsertClient
func (c *InsertCHWrapper) Exec(ctx context.Context, query string, args ...any) error {
	//return c.Exec(ctx, query, args)
	return errors.New("not implemented")
}

func (c *InsertCHWrapper) Scan(ctx context.Context, req string, args []any, dest ...interface{}) error {
	return errors.New("not implemented")
}

func (c *InsertCHWrapper) DropIfEmpty(ctx context.Context, name string) error {
	return errors.New("not implemented")
}

func (c *InsertCHWrapper) TableExists(ctx context.Context, name string) (bool, error) {
	return false, errors.New("not implemented")
}

func (c *InsertCHWrapper) GetDBExec(env map[string]string) func(ctx context.Context, query string, args ...[]interface{}) error {
	return func(ctx context.Context, query string, args ...[]interface{}) error {
		return errors.New("not implemented")
	}
}

func (c *InsertCHWrapper) GetVersion(ctx context.Context, k uint64) (uint64, error) {
	return 0, errors.New("not implemented")
}

func (c *InsertCHWrapper) GetSetting(ctx context.Context, tp string, name string) (string, error) {
	return "", errors.New("not implemented")
}

func (c *InsertCHWrapper) PutSetting(ctx context.Context, tp string, name string, value string) error {
	return errors.New("not implemented")
}

func (c *InsertCHWrapper) GetFirst(req string, first ...interface{}) error {
	return errors.New("not implemented")
}

func (c *InsertCHWrapper) GetList(req string) ([]string, error) {
	return nil, errors.New("not implemented")
}
